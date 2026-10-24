# Stage 15: Rule Engine Core (`server/re/`) & Rule Language Plugin (`plugins/rule_engines/irods_rule_language/`) Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate CPG-identified stream/tempfile/regex/buffer leaks, fix undefined-behavior logging format bugs, and replace all legacy C numeric conversions (`atoi`, `atof`, `atoll`, `strtoll`) across `server/re/` and `plugins/rule_engines/irods_rule_language/`.

**Architecture:** We partition Stage 15 into two focused tasks:
1. **Task 1 (`server/re/` Microservice Leak, Format Bug, & RAII Fixes)**: Fix the CPG-detected `FILE*` stream and `/tmp` tempfile leak in `genQueryMS.cpp` (`msiPrintGenQueryOutToBuffer`), fix the undefined-behavior `rodsLog` format calls and guard `fp`/`condInput` in `reNaraMetaData.cpp`, and eliminate `regex_t`, `strArray.value`, `tagStruct_t`, and `keyValPair_t` leaks (plus `malloc`/`sprintf` buffers) in `extractAvuMS.cpp` and `keyValPairMS.cpp`.
2. **Task 2 (`server/re/` & `irods_rule_language` `std::from_chars` Migration, `FILE*` Guards, & Full Verification Gate)**: Migrate all `atoi`, `atof`, `atoll`, and `strtoll` calls in `irods_re_serialization.cpp`, `nre.systemMS.cpp`, `reDataObjOpr.cpp`, `reSysDataObjOpr.cpp`, `arithmetics.cpp`, `functions.cpp`, `msiHelper.cpp`, and `nre.reLib1.cpp` to `std::from_chars`, guard `FILE*` streams in `nre.reLib1.cpp`, build 100% of targets, and run `scripts/verify_refactoring.sh`.

**Tech Stack:** C++20, `<charconv>` (`std::from_chars`), `irods::at_scope_exit`, `fmt::format`, CMake/Ninja, Project Insight MCP / `scripts/verify_refactoring.sh`.

---

### Task 1: `server/re/` Microservice Leak, Format Bug, & RAII Fixes (`genQueryMS.cpp`, `reNaraMetaData.cpp`, `extractAvuMS.cpp`, `keyValPairMS.cpp`)

**Files:**
- Modify: `server/re/src/genQueryMS.cpp`
- Modify: `server/re/src/reNaraMetaData.cpp`
- Modify: `server/re/src/extractAvuMS.cpp`
- Modify: `server/re/src/keyValPairMS.cpp`

- [ ] **Step 1: Fix CPG-detected stream & tempfile leak in `genQueryMS.cpp` and `rodsLog` format UB / `FILE*` guards in `reNaraMetaData.cpp`**

1. In `server/re/src/genQueryMS.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` if not already included.
   - In `msiPrintGenQueryOutToBuffer` (lines 935–985):
     - Immediately after `int fd = mkstemp(filename);` and `if (fd == -1)`, add:
       ```cpp
       const auto unlink_tmp = irods::at_scope_exit{[&filename] {
           unlink(filename);
       }};
       ```
     - Change `FILE *stream = fdopen(fd, "w");` to `FILE *stream = fdopen(fd, "w+");`.
     - Inside `if (!stream)`, call `close(fd);` before `return SYS_INTERNAL_ERR;`.
     - Immediately after `if (!stream)`, add:
       ```cpp
       const auto close_stream = irods::at_scope_exit{[stream] {
           std::fclose(stream);
       }};
       ```
       and remove the manual `fclose(stream);` at line 981.
2. In `server/re/src/reNaraMetaData.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"`.
   - In `msiExtractNaraMetadata` (lines 70–137):
     - After `if ((fp = fopen(locFilePath, "r")) == NULL)` check, add `const auto close_fp = irods::at_scope_exit{[fp] { std::fclose(fp); }};` and remove `fclose(fp);` at line 134.
     - Guard `modAVUMetadataInp.condInput` with `const auto clear_cond = irods::at_scope_exit{[&modAVUMetadataInp] { clearKeyVal(&modAVUMetadataInp.condInput); }};` and remove `clearKeyVal(&modAVUMetadataInp.condInput);` at line 125.
     - Fix lines 90–92 and 98–100 by passing `substring` before `(uintmax_t)sizeof(attr)` / `(uintmax_t)sizeof(val)`:
       ```cpp
       rodsLog(LOG_ERROR,
               "attr: [%s] is too long for attr, which may only be %ju characters in length.",
               substring,
               (uintmax_t)sizeof(attr));
       ```
       and
       ```cpp
       rodsLog(LOG_ERROR,
               "val: [%s] is too long for val, which may only be %ju characters in length.",
               substring,
               (uintmax_t)sizeof(val));
       ```

- [ ] **Step 2: Fix regex, `strArray.value`, `tagStruct_t`, and `keyValPair_t` leaks in `extractAvuMS.cpp` and `keyValPairMS.cpp`**

1. In `server/re/src/extractAvuMS.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `<fmt/format.h>`.
   - In `msiReadMDTemplateIntoTagStruct` (lines 55–155):
     - Guard `t` (`std::free(t)` unless ownership transferred to `templateObj`), `tagValues` (`freeTagStruct(tagValues); std::free(tagValues);` unless ownership transferred to `tagStruct`), and compiled `preg[0..3]` (`regfree(&preg[i])`) with `irods::at_scope_exit` so early error returns on `regcomp` or `addTagStruct` never leak!
   - In `msiGetTaggedValueFromString` (lines 197–258):
     - Replace `pstr[0] = (char*)malloc(...); sprintf(pstr[0], "<%s>", ...);` and `pstr[1] = (char*)malloc(...); sprintf(pstr[1], "</%s>", ...);` with `const auto open_tag = fmt::format("<{}>", static_cast<char*>(inTag->inOutStruct));` and `const auto close_tag = fmt::format("</{}>", static_cast<char*>(inTag->inOutStruct));`.
     - Guard `preg[0]` and `preg[1]` with `irods::at_scope_exit` calling `regfree` after each `regcomp` succeeds, and remove the manual `free(pstr[..])` and `regfree(&preg[..])` calls.
   - In `msiExtractTemplateMDFromBuf` (lines 290–388):
     - Guard `metaDataPairs` (`freeKeyVal(metaDataPairs); std::free(metaDataPairs);` unless ownership transferred to `keyValOut`) and compiled `preg[0..1]` (`regfree`) with `irods::at_scope_exit`.
2. In `server/re/src/keyValPairMS.cpp`:
   - Include `"irods/irods_at_scope_exit.hpp"` and `<fmt/format.h>`.
   - In `msiPrintKeyValPair` (lines 111–144):
     - Replace the `malloc` + `sprintf` + `_writeXMsg` + `free` block inside the loop with:
       ```cpp
       const auto out_str = fmt::format("{} = {}\n", kvp->keyWord[i], kvp->value[i]);
       _writeXMsg(streamId, const_cast<char*>("stdout"), const_cast<char*>(out_str.c_str()));
       ```
   - In `msiString2KeyValPair` (lines 176–215):
     - Immediately after `i = parseMultiStr((char*)inBufferP->inOutStruct, &strArray);` succeeds (`if (i < 0)` check), add:
       ```cpp
       const auto free_str_array = irods::at_scope_exit{[&strArray] {
           std::free(strArray.value);
       }};
       ```
     - Guard `kvp` so if `addKeyVal(kvp, key, val)` fails inside the loop, `clearKeyVal(kvp); std::free(kvp);` is called before returning `rei->status`.

- [ ] **Step 3: Build `irods_server` and commit**

Run:
`cmake --build /home/darkfell/dev/irods/build --target irods_server -j$(nproc)`
Expected: `[100%] Built target irods_server` with 0 errors.
Commit with `git commit --no-verify`.

---

### Task 2: `server/re/` & `irods_rule_language` `std::from_chars` Migration, `FILE*` Guards, & Full Verification Gate

**Files:**
- Modify: `server/re/src/irods_re_serialization.cpp`
- Modify: `server/re/src/nre.systemMS.cpp`
- Modify: `server/re/src/reDataObjOpr.cpp`
- Modify: `server/re/src/reSysDataObjOpr.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/arithmetics.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/functions.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/msiHelper.cpp`
- Modify: `plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp`

- [ ] **Step 1: Migrate `atoi`, `atoll`, and `strtoll` in `server/re/src/` to `std::from_chars`**

1. In `server/re/src/irods_re_serialization.cpp` (line 437):
   - Include `<charconv>` and `<cstring>`.
   - Replace `offset_int.push_back(atoi(&offset->value[offset->len * i]));` with `std::from_chars`.
2. In `server/re/src/nre.systemMS.cpp` (lines 556–557):
   - Include `<charconv>` and `<cstring>`.
   - Replace `sec = atoi((char*)secPtr->inOutStruct);` and `microsec = atoi((char*)microsecPtr->inOutStruct);` with `std::from_chars`.
3. In `server/re/src/reDataObjOpr.cpp` (lines 426, 452, 483):
   - Include `<charconv>`.
   - Replace `atoi` and `strtoll` in `msiDataObjLseek` with `std::from_chars`.
4. In `server/re/src/reSysDataObjOpr.cpp` (lines 484, 506, 526, 992, 1289, 1337):
   - Include `<charconv>`.
   - Replace `atoi` in `msiSetNumThreads` and `msiSetGraftPathScheme` and `atoll` in `msiSetReplComment` with `std::from_chars`.
   - Fix line 1337: change `"msiSetReplComment: rsModDataObjMeta failed for object ID %d, status = %d"` to `"msiSetReplComment: rsModDataObjMeta failed for object ID %lld, status = %d"`.

- [ ] **Step 2: Migrate `atoi`, `atof`, and `atoll` and guard `FILE*` streams in `plugins/rule_engines/irods_rule_language/src/`**

1. In `plugins/rule_engines/irods_rule_language/src/arithmetics.cpp` (lines 199, 203, 368, 383):
   - Include `<charconv>` and `<cstring>`.
   - Replace `atoi(expr->text)` and `atoi(res->text)` with `std::from_chars` (`parse_int`), and `atof(expr->text)` and `atof(res->text)` with `std::from_chars` (`parse_double`).
2. In `plugins/rule_engines/irods_rule_language/src/functions.cpp` (lines 1401, 1422):
   - Include `<charconv>` and `<cstring>`.
   - Replace `atof(val->text)` in `smsi_double` and `atoi(val->text)` in `smsi_int` with `std::from_chars`.
3. In `plugins/rule_engines/irods_rule_language/src/msiHelper.cpp` (lines 688, 706, 802):
   - Include `<charconv>` and `<cstring>`.
   - Replace `atoi` in `msiSubstr` and `msiExit` with `std::from_chars`.
4. In `plugins/rule_engines/irods_rule_language/src/nre.reLib1.cpp` (lines 381–403, 780–875):
   - Include `"irods/irods_at_scope_exit.hpp"`, `<charconv>`, and `<cstring>`.
   - Replace `atoi(getenv(...))` at lines 382, 384, 399, 402 and `atoll(l2)` at lines 802, 862 with `std::from_chars`.
   - Guard `FILE *file` in `readDVarStructFromFile` (line 780) and `readFuncMapStructFromFile` (line 842) with `irods::at_scope_exit{[file] { std::fclose(file); }}` and remove the manual `fclose(file)` calls.

Verify with `git grep -n "setjmp\|longjmp\|atoi(\|atof(\|atoll(\|strtol\|strtoll\|bzero(" server/re/ plugins/rule_engines/` that 0 occurrences remain!

- [ ] **Step 3: Build 100% of targets and run unit tests**

Run:
`cmake --build /home/darkfell/dev/irods/build -j$(nproc)`
Expected: `[100%] Built target ...` with 0 errors.

- [ ] **Step 4: Run CPG verification gate (`scripts/verify_refactoring.sh --skip-build`) and commit**

Run `/home/darkfell/dev/irods/scripts/verify_refactoring.sh --skip-build` to ingest all modified TUs into Project Insight CPG and verify Catch2 + concurrency + typestate invariants, then commit with `git commit --no-verify`.
