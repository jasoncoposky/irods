# Stage 15: Rule Engine Core (`server/re/`) & Rule Language Plugin (`plugins/rule_engines/irods_rule_language/`) Modernization — Design Specification

## 1. Executive Summary & Objective
Stage 15 of the iRODS Master Modernization Roadmap modernizes the Rule Engine Core (`server/re/`) and the Native Rule Language Plugin (`plugins/rule_engines/irods_rule_language/`). Using the Project Insight Code Property Graph (CPG) typestate analysis and a comprehensive codebase audit of `server/re/` and `plugins/rule_engines/irods_rule_language/`, we identified:
1. **CPG-Detected Stream & Temporary File Leak in `server/re/src/genQueryMS.cpp` (`msiPrintGenQueryOutToBuffer`)**:
   - `mkstemp(filename)` creates `/tmp/msiPrintGenQueryOutToBufferXXXXXX`, which is **never unlinked** on any return path (leaking temporary files into `/tmp` on every invocation).
   - If `fdopen(fd, "w")` fails, `fd` is leaked.
   - `fdopen(fd, "w")` opens a write-only `FILE*` stream even though line 968 calls `rewind(stream)` and `fgets(buffer, ..., stream)` to read the written output back into `outParam`!
2. **Undefined-Behavior `rodsLog` Format Bug & `FILE*`/`condInput` Leak Risks in `server/re/src/reNaraMetaData.cpp`**:
   - Lines 90–92 and 98–100 call `rodsLog(LOG_ERROR, "attr: [%s] is too long for attr, which may only be %ju characters in length.", (uintmax_t)sizeof(attr))` — passing `sizeof(attr)` (`size_t`) to `%s` and reading garbage off the stack for `%ju`!
   - `FILE *fp` and `modAVUMetadataInp.condInput` rely on manual cleanup at the end of `msiExtractNaraMetadata`.
3. **Regex, `strArray`, and Heap Struct Leaks in `server/re/src/extractAvuMS.cpp` and `server/re/src/keyValPairMS.cpp`**:
   - In `msiReadMDTemplateIntoTagStruct` (`extractAvuMS.cpp`), early returns on `regcomp(&preg[1..3])` or `addTagStruct` failure leak compiled `regex_t` instances (`regfree`), `t`, and `tagValues`.
   - In `msiGetTaggedValueFromString` (`extractAvuMS.cpp`), `malloc` + `sprintf` + `free` for `pstr[0..1]` and `regfree(&preg[0..1])` can be replaced with `fmt::format` and `irods::at_scope_exit`.
   - In `msiExtractTemplateMDFromBuf` (`extractAvuMS.cpp`), early returns on `regcomp(&preg[1])` or `addKeyVal` failure leak `preg[0..1]` and `metaDataPairs`.
   - In `msiPrintKeyValPair` (`keyValPairMS.cpp`), a manual `malloc` + `sprintf` + `free` loop inside the `keyValPair_t` iteration can be replaced with `fmt::format`.
   - In `msiString2KeyValPair` (`keyValPairMS.cpp`), `strArray.value` allocated by `parseMultiStr` is **never freed** on any path (`std::free(strArray.value)` is completely missing!), and `kvp` is leaked if `addKeyVal` fails.
4. **Legacy C Numeric Conversions (`atoi`, `atof`, `atoll`, `strtoll`) Across `server/re/src/` and `plugins/rule_engines/irods_rule_language/src/`**:
   - `server/re/src/`: `irods_re_serialization.cpp`, `nre.systemMS.cpp`, `reDataObjOpr.cpp`, `reSysDataObjOpr.cpp` (plus fixing `%d` format specifier for `rodsLong_t dataObjInfo.dataId` at line 1337 of `reSysDataObjOpr.cpp`).
   - `plugins/rule_engines/irods_rule_language/src/`: `arithmetics.cpp`, `functions.cpp`, `msiHelper.cpp`, `nre.reLib1.cpp` (plus `irods::at_scope_exit` guards for `FILE* file` in `readDVarStructFromFile` and `readFuncMapStructFromFile`).

---

## 2. Detailed Component Design

### Component 1: `server/re/` Microservice Leak, Format Bug, & RAII Fixes (`genQueryMS.cpp`, `reNaraMetaData.cpp`, `extractAvuMS.cpp`, `keyValPairMS.cpp`)
- **`server/re/src/genQueryMS.cpp`**:
  - In `msiPrintGenQueryOutToBuffer`:
    - Immediately after `int fd = mkstemp(filename);` and `if (fd == -1)`, guard `filename` with `const auto unlink_tmp = irods::at_scope_exit{[&filename] { unlink(filename); }};`.
    - Open `FILE *stream = fdopen(fd, "w+");` (read/write mode so `rewind` + `fgets` is valid). If `!stream`, call `close(fd);` before returning `SYS_INTERNAL_ERR`.
    - Guard `stream` with `const auto close_stream = irods::at_scope_exit{[stream] { std::fclose(stream); }};` and remove the manual `fclose(stream)` at the end.
- **`server/re/src/reNaraMetaData.cpp`**:
  - In `msiExtractNaraMetadata`:
    - Guard `fp` with `const auto close_fp = irods::at_scope_exit{[fp] { std::fclose(fp); }};` after `fopen` succeeds, and remove the manual `fclose(fp)` at line 134.
    - Guard `modAVUMetadataInp.condInput` with `const auto clear_cond = irods::at_scope_exit{[&modAVUMetadataInp] { clearKeyVal(&modAVUMetadataInp.condInput); }};` and remove the manual `clearKeyVal` at line 125.
    - Fix the two undefined-behavior `rodsLog` format calls at lines 90–92 and 98–100 by passing `substring` as the `%s` argument before `(uintmax_t)sizeof(attr)` / `(uintmax_t)sizeof(val)`.
- **`server/re/src/extractAvuMS.cpp`**:
  - In `msiReadMDTemplateIntoTagStruct`:
    - Guard `t` (`free(t)` if ownership not transferred to `templateObj`) and `tagValues` (`freeTagStruct(tagValues)` if ownership not transferred to `tagStruct`) with `irods::at_scope_exit`.
    - Track compiled `preg[0..3]` and guard with `irods::at_scope_exit` so `regfree` is called for all initialized regexes on any return path.
  - In `msiGetTaggedValueFromString`:
    - Replace `malloc` + `sprintf` + `free` of `pstr[0]` and `pstr[1]` with `fmt::format("<{}>", (char*)inTag->inOutStruct)` and `fmt::format("</{}>", (char*)inTag->inOutStruct)`.
    - Guard `preg[0]` and `preg[1]` with `irods::at_scope_exit` calling `regfree`.
  - In `msiExtractTemplateMDFromBuf`:
    - Guard `metaDataPairs` (`freeKeyVal(metaDataPairs)` if ownership not transferred to `keyValOut`) and compiled `preg[0..1]` (`regfree`) with `irods::at_scope_exit`.
- **`server/re/src/keyValPairMS.cpp`**:
  - In `msiPrintKeyValPair`:
    - Replace `malloc` + `sprintf` + `free` inside the loop with `const auto line = fmt::format("{} = {}\n", kvp->keyWord[i], kvp->value[i]); _writeXMsg(streamId, const_cast<char*>("stdout"), const_cast<char*>(line.c_str()));`.
  - In `msiString2KeyValPair`:
    - Guard `strArray.value` with `const auto free_str_array = irods::at_scope_exit{[&strArray] { std::free(strArray.value); }};` immediately after `parseMultiStr` succeeds!
    - Guard `kvp` with `irods::at_scope_exit` if `addKeyVal` fails before ownership is transferred to `outKeyValPairP`.

### Component 2: `server/re/` & `plugins/rule_engines/irods_rule_language/` Numeric Conversion (`std::from_chars`) & `FILE*` Guard Modernization
- **`server/re/src/irods_re_serialization.cpp`**:
  - Replace `atoi(&offset->value[offset->len * i])` (line 437) with `std::from_chars`.
- **`server/re/src/nre.systemMS.cpp`**:
  - Replace `atoi` in `msiSleep` (lines 556–557) with `std::from_chars`.
- **`server/re/src/reDataObjOpr.cpp`**:
  - Replace `atoi` and `strtoll` in `msiDataObjLseek` (lines 426, 452, 483) with `std::from_chars`.
- **`server/re/src/reSysDataObjOpr.cpp`**:
  - Replace `atoi` in `msiSetNumThreads` (lines 484, 506, 526) and `msiSetGraftPathScheme` (line 992) and `atoll` in `msiSetReplComment` (line 1289) with `std::from_chars`.
  - Fix `%d` format specifier for `rodsLong_t dataObjInfo.dataId` at line 1337 to `%lld`.
- **`plugins/rule_engines/irods_rule_language/src/arithmetics.cpp`**:
  - Replace `atoi` and `atof` at lines 199, 203, 368, 383 with `std::from_chars`.
- **`plugins/rule_engines/irods_rule_language/src/functions.cpp`**:
  - Replace `atof` in `smsi_double` (line 1401) and `atoi` in `smsi_int` (line 1422) with `std::from_chars`.
- **`plugins/rule_engines/irods_rule_language/src/msiHelper.cpp`**:
  - Replace `atoi` in `msiSubstr` (lines 688, 706) and `msiExit` (line 802) with `std::from_chars`.
- **`plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp`**:
  - Replace `atoi` on `getenv(...)` (lines 382, 384, 399, 402) and `atoll(l2)` (lines 802, 862) with `std::from_chars`.
  - Guard `FILE *file` in `readDVarStructFromFile` (line 780) and `readFuncMapStructFromFile` (line 842) with `irods::at_scope_exit`.

---

## 3. Verification Criteria
1. Zero occurrences of `setjmp`, `longjmp`, `atoi(`, `atof(`, `atoll(`, `strtol`, `strtoll`, `bzero(` across `server/re/` and `plugins/rule_engines/`.
2. 100% build of `irods_server` and `irods_rule_engine_plugin-irods_rule_language`.
3. Full pass of `scripts/verify_refactoring.sh` (Build + Catch2 unit tests + Project Insight CPG ingestion + concurrency + typestate verification, confirming the `genQueryMS.cpp` typestate violation is resolved).
