#include "irods/wire_transcoder.hpp"

#include "irods/alignPointer.hpp"
#include "irods/irods_at_scope_exit.hpp"
#include "irods/irods_pack_table.hpp"
#include "irods/rcGlobalExtern.h"
#include "irods/rcMisc.h"
#include "irods/rodsErrorTable.h"
#include "irods/rodsLog.h"
#include "irods/rodsPackInstruct.h"
#include "irods/version.hpp"

#include "exception.hpp"
#include "json.hpp"
#include "node.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace irods::wire
{
    namespace
    {
        template <typename T = int>
        auto parse_int_val(const char* _s, bool _auto_base = false) noexcept -> T
        {
            if (!_s) {
                return 0;
            }
            std::string_view sv{_s};
            while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front()))) {
                sv.remove_prefix(1);
            }
            if (!sv.empty() && sv.front() == '+') {
                sv.remove_prefix(1);
            }
            int base = 10;
            if (_auto_base && sv.size() > 2 && sv[0] == '0' && (sv[1] == 'x' || sv[1] == 'X')) {
                base = 16;
                sv.remove_prefix(2);
            }
            T val{};
            if (!sv.empty()) {
                std::from_chars(sv.data(), sv.data() + sv.size(), val, base);
            }
            return val;
        }

        void free_packed_item_chain(packItem_t& _head) noexcept
        {
            std::free(_head.name);
            _head.name = nullptr;
            packItem_t* cur = _head.next;
            _head.next = nullptr;
            while (cur) {
                packItem_t* next = cur->next;
                std::free(cur->name);
                std::free(cur);
                cur = next;
            }
        }

        auto copy_str_from_pi_buf(const char*& _in_buf, char* _out_buf, int _dependent_flag) noexcept -> int
        {
            const char* in_ptr = _in_buf;
            char* out_ptr = _out_buf;
            int out_len = 0;
            int c = 0;

            while ((c = *in_ptr) != '\0') {
                if (_dependent_flag > 0) {
                    if (c == ';') {
                        break;
                    }
                    if (out_len > 0 || !std::isspace(static_cast<unsigned char>(c))) {
                        *out_ptr++ = static_cast<char>(c);
                        ++out_len;
                    }
                    ++in_ptr;
                }
                else if (std::isspace(static_cast<unsigned char>(c))) {
                    ++in_ptr;
                    if (out_len > 0) {
                        break;
                    }
                }
                else if (c == '*' || c == ';' || c == '?' || c == '$' || c == '#') {
                    if (out_len > 0) {
                        break;
                    }
                    *out_ptr++ = static_cast<char>(c);
                    ++in_ptr;
                    ++out_len;
                    break;
                }
                else {
                    *out_ptr++ = static_cast<char>(c);
                    ++in_ptr;
                    ++out_len;
                }
            }

            *out_ptr = '\0';
            _in_buf = in_ptr;
            return out_len;
        }

        auto pack_type_lookup(const char* _type_name) noexcept -> int
        {
            for (int i = 0; i < NumOfPackTypes; ++i) {
                if (std::strcmp(_type_name, packTypeTable[i].name) == 0) {
                    return i;
                }
            }
            return -1;
        }

        auto parse_pack_instruct(const char* _pack_instruct, packItem_t& _pack_item_head) -> int
        {
            char buf[MAX_PI_LEN]{};
            packItem_t* my_pack_item = &_pack_item_head;
            packItem_t* prev_pack_item = nullptr;
            const char* inptr = _pack_instruct;
            int got_type_cast = 0;
            int got_item_name = 0;

            const auto free_cur_item = [&]() noexcept {
                if (my_pack_item) {
                    std::free(my_pack_item->name);
                    my_pack_item->name = nullptr;
                    if (my_pack_item != &_pack_item_head) {
                        std::free(my_pack_item);
                    }
                    my_pack_item = nullptr;
                }
            };

            while (copy_str_from_pi_buf(inptr, buf, 0) > 0) {
                if (!my_pack_item) {
                    my_pack_item = static_cast<packItem_t*>(std::calloc(1, sizeof(packItem_t)));
                    if (!my_pack_item) {
                        return SYS_MALLOC_ERR;
                    }
                }

                if (std::strcmp(buf, ";") == 0) {
                    if (got_type_cast == 0 && got_item_name == 0) {
                        continue;
                    }
                    if (got_type_cast > 0 && got_item_name == 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    got_type_cast = 0;
                    got_item_name = 0;
                    if (prev_pack_item != nullptr) {
                        prev_pack_item->next = my_pack_item;
                        my_pack_item->prev = prev_pack_item;
                    }
                    prev_pack_item = my_pack_item;
                    my_pack_item = nullptr;
                    continue;
                }
                if (std::strcmp(buf, "%") == 0) {
                    if (got_type_cast > 0 || got_item_name > 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    my_pack_item->typeInx = static_cast<packTypeInx_t>(pack_type_lookup(buf));
                    if (my_pack_item->typeInx < 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    got_type_cast = 1;
                    const int out_len = copy_str_from_pi_buf(inptr, buf, 1);
                    if (out_len <= 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    my_pack_item->name = strdup(buf);
                    got_item_name = 1;
                    continue;
                }
                if (std::strcmp(buf, "?") == 0) {
                    if (got_type_cast > 0 || got_item_name > 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    my_pack_item->typeInx = static_cast<packTypeInx_t>(pack_type_lookup(buf));
                    if (my_pack_item->typeInx < 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    got_type_cast = 1;
                    const int out_len = copy_str_from_pi_buf(inptr, buf, 0);
                    if (out_len <= 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    rstrcpy(my_pack_item->strValue, buf, NAME_LEN);
                    continue;
                }
                if (std::strcmp(buf, "*") == 0) {
                    my_pack_item->pointerType = A_POINTER;
                    if (got_type_cast == 0 || got_item_name > 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    continue;
                }
                if (std::strcmp(buf, "#") == 0) {
                    my_pack_item->pointerType = NO_PACK_POINTER;
                    if (got_type_cast == 0 || got_item_name > 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    continue;
                }
                if (std::strcmp(buf, "$") == 0) {
                    my_pack_item->pointerType = NO_FREE_POINTER;
                    if (got_type_cast == 0 || got_item_name > 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    continue;
                }
                if (got_type_cast == 0) {
                    my_pack_item->typeInx = static_cast<packTypeInx_t>(pack_type_lookup(buf));
                    if (my_pack_item->typeInx < 0) {
                        free_cur_item();
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    got_type_cast = 1;
                    continue;
                }
                if (got_type_cast == 1 && got_item_name == 0) {
                    my_pack_item->name = strdup(buf);
                    got_item_name = 1;
                    continue;
                }

                free_cur_item();
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }

            if (my_pack_item != nullptr) {
                free_cur_item();
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }

            return 0;
        }

        auto resolve_int_in_item(const char* _name, const packItem_t& _item) noexcept -> int
        {
            if (isAllDigit(_name)) {
                return parse_int_val(_name);
            }

            const packItem_t* tmp = _item.prev;
            while (tmp != nullptr) {
                if (tmp->name && std::strcmp(_name, tmp->name) == 0 &&
                    packTypeTable[tmp->typeInx].number == PACK_INT_TYPE)
                {
                    return tmp->intValue;
                }
                if (tmp->prev == nullptr && tmp->parent != nullptr) {
                    tmp = tmp->parent;
                }
                else {
                    tmp = tmp->prev;
                }
            }

            for (int i = 0; std::strcmp(PackConstantTable[i].name, PACK_TABLE_END_PI) != 0; ++i) {
                if (std::strcmp(PackConstantTable[i].name, _name) == 0) {
                    return PackConstantTable[i].value;
                }
            }

            return SYS_PACK_INSTRUCT_FORMAT_ERR;
        }

        auto resolve_int_dep_item(packItem_t& _item) -> int
        {
            char buf[MAX_NAME_LEN]{};
            char my_pi[MAX_NAME_LEN]{};
            int end_reached = 0;
            int out_len = 0;

            const char* tmp_ptr = _item.name;
            char* buf_ptr = buf;
            int c = 0;

            while ((c = *tmp_ptr) != '\0') {
                if (c == ' ' || c == '\t' || c == '\n' || c == ':') {
                    if (out_len == 0) {
                        ++tmp_ptr;
                        continue;
                    }
                    if (end_reached == 0) {
                        *buf_ptr = '\0';
                        end_reached = 1;
                    }
                    if (c == ':') {
                        ++tmp_ptr;
                        break;
                    }
                }
                else if (c == ';') {
                    return SYS_PACK_INSTRUCT_FORMAT_ERR;
                }
                else {
                    *buf_ptr++ = static_cast<char>(c);
                    ++out_len;
                }
                ++tmp_ptr;
            }

            const int key_val = resolve_int_in_item(buf, _item);
            if (key_val == SYS_PACK_INSTRUCT_FORMAT_ERR) {
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }

            buf_ptr = buf;
            while ((c = *tmp_ptr) != '\0') {
                if (c == ',' || c == '=') {
                    *buf_ptr = '\0';
                    if (std::strstr(buf, "default") != nullptr || parse_int_val(buf) == key_val) {
                        if (c == ',') {
                            while (true) {
                                c = *tmp_ptr;
                                if (c == '=') {
                                    break;
                                }
                                if (c == ';' || c == ':' || c == '\0') {
                                    return SYS_PACK_INSTRUCT_FORMAT_ERR;
                                }
                                ++tmp_ptr;
                            }
                        }
                        ++tmp_ptr;
                        char* pf_ptr = my_pi;
                        while (true) {
                            c = *tmp_ptr;
                            if (c == '\0' || c == ':' || c == ';') {
                                *pf_ptr++ = ';';
                                *pf_ptr = '\0';
                                break;
                            }
                            *pf_ptr++ = *tmp_ptr++;
                        }
                        break;
                    }
                    buf_ptr = buf;
                    if (c == '=') {
                        ++tmp_ptr;
                        while (true) {
                            c = *tmp_ptr;
                            if (c == ':') {
                                break;
                            }
                            if (c == '\0' || c == ';') {
                                return SYS_PACK_INSTRUCT_FORMAT_ERR;
                            }
                            ++tmp_ptr;
                        }
                    }
                }
                else {
                    *buf_ptr++ = static_cast<char>(c);
                }
                ++tmp_ptr;
            }

            packItem_t new_item{};
            const int status = parse_pack_instruct(my_pi, new_item);
            if (status < 0) {
                free_packed_item_chain(new_item);
                return status;
            }

            std::free(_item.name);
            packItem_t* last_item = &new_item;
            while (last_item->next) {
                last_item = last_item->next;
            }
            if (new_item.next) {
                new_item.next->prev = &_item;
            }
            new_item.prev = _item.prev;
            new_item.parent = _item.parent;
            last_item->next = _item.next;
            if (_item.next) {
                _item.next->prev = last_item;
            }
            _item = new_item;
            return 0;
        }

        auto resolve_str_in_item(packItem_t& _item) -> int
        {
            const char* name = _item.strValue;
            const packItem_t* tmp = _item.prev;
            while (tmp != nullptr) {
                if (tmp->name && std::strcmp(name, tmp->name) == 0 &&
                    packTypeTable[tmp->typeInx].number == PACK_PI_STR_TYPE)
                {
                    break;
                }
                if (tmp->prev == nullptr && tmp->parent != nullptr) {
                    tmp = tmp->parent;
                }
                else {
                    tmp = tmp->prev;
                }
            }

            if (tmp == nullptr) {
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }

            _item.typeInx = PACK_STRUCT_TYPE;
            std::free(_item.name);
            _item.name = strdup(tmp->strValue);
            return 0;
        }

        auto resolve_dep_in_array(packItem_t& _item) -> int
        {
            _item.dim = _item.hintDim = 0;
            char open_sym = '\0';
            std::string buffer;

            for (std::size_t idx = 0; _item.name && _item.name[idx] != '\0'; ++idx) {
                const char c = _item.name[idx];
                if (c == '[' || c == '(') {
                    if (open_sym) {
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    if ((c == '[' && _item.dim >= MAX_PACK_DIM) ||
                        (c == '(' && _item.hintDim >= MAX_PACK_DIM))
                    {
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    open_sym = c;
                    _item.name[idx] = '\0';
                    buffer.clear();
                }
                else if (c == ']' || c == ')') {
                    if ((c == ']' && open_sym != '[') || (c == ')' && open_sym != '(') || buffer.empty()) {
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                    open_sym = '\0';
                    int& dim_sz = (c == ']') ? _item.dimSize[_item.dim++] : _item.hintDimSize[_item.hintDim++];
                    dim_sz = resolve_int_in_item(buffer.c_str(), _item);
                    if (dim_sz < 0) {
                        return SYS_PACK_INSTRUCT_FORMAT_ERR;
                    }
                }
                else if (open_sym) {
                    buffer += c;
                }
            }
            return 0;
        }

        auto resolve_packed_item(packItem_t& _item, const void*& _in_ptr, packOpr_t _pack_opr) -> int
        {
            if (_item.typeInx == PACK_DEPENDENT_TYPE) {
                if (const int ec = resolve_str_in_item(_item); ec < 0) {
                    return ec;
                }
            }
            else if (_item.typeInx == PACK_INT_DEPENDENT_TYPE) {
                if (const int ec = resolve_int_dep_item(_item); ec < 0) {
                    return ec;
                }
            }

            if (const int ec = resolve_dep_in_array(_item); ec < 0) {
                return ec;
            }

            const void* ptr = _in_ptr;
            if (_item.pointerType > 0 && _pack_opr == PACK_OPR) {
                ptr = ialignAddr(ptr);
                if (ptr != nullptr) {
                    _item.pointer = *static_cast<const void* const*>(ptr);
                    ptr = static_cast<const char*>(ptr) + sizeof(void*);
                }
                else {
                    _item.pointer = nullptr;
                }
            }

            if (!_item.name || std::strlen(_item.name) == 0) {
                if (_item.pointerType == 0 || _item.pointer != nullptr) {
                    return SYS_PACK_INSTRUCT_FORMAT_ERR;
                }
                std::free(_item.name);
                _item.name = strdup("STR_PTR_PI");
            }

            _in_ptr = ptr;
            return 0;
        }

        auto match_pack_instruct(const char* _name, const packInstruct_t* _my_pack_table) -> const char*
        {
            if (_my_pack_table != nullptr) {
                for (int i = 0; std::strcmp(_my_pack_table[i].name, PACK_TABLE_END_PI) != 0; ++i) {
                    if (std::strcmp(_my_pack_table[i].name, _name) == 0) {
                        return _my_pack_table[i].packInstruct;
                    }
                }
            }

            for (int i = 0; std::strcmp(RodsPackTable[i].name, PACK_TABLE_END_PI) != 0; ++i) {
                if (std::strcmp(RodsPackTable[i].name, _name) == 0) {
                    return RodsPackTable[i].packInstruct;
                }
            }

            auto& pk_tbl = irods::get_pack_table();
            if (auto itr = pk_tbl.find(_name); itr != pk_tbl.end()) {
                return itr->second.packInstruct.c_str();
            }

            return nullptr;
        }

        auto get_num_element(const packItem_t& _item) noexcept -> int
        {
            int n = 1;
            for (int i = 0; i < _item.dim; ++i) {
                n *= _item.dimSize[i];
            }
            return n;
        }

        auto get_num_hint_element(const packItem_t& _item) noexcept -> int
        {
            int n = 1;
            for (int i = 0; i < _item.hintDim; ++i) {
                n *= _item.hintDimSize[i];
            }
            return n;
        }

        void get_num_str_and_str_len(const packItem_t& _item, int& _num_str, int& _max_str_len) noexcept
        {
            if (_item.hintDim <= 0) {
                _max_str_len = -1;
                _num_str = 1;
            }
            else {
                _max_str_len = _item.hintDimSize[_item.hintDim - 1];
                if (_max_str_len <= 0) {
                    _num_str = 0;
                }
                else {
                    _num_str = get_num_hint_element(_item) / _max_str_len;
                }
            }
        }

        auto make_field_key(const char* _raw_name, std::unordered_map<std::string, int>& _counts) -> std::string
        {
            std::string base{_raw_name ? _raw_name : ""};
            const int idx = _counts[base]++;
            if (idx == 0) {
                return base;
            }
            return fmt::format("{}#{}", base, idx);
        }

        auto read_one_c_str(const void*& _in_ptr, int _max_str_len, std::string_view& _out_sv) noexcept -> int
        {
            if (!_in_ptr) {
                _out_sv = {};
                return 0;
            }
            const char* s = static_cast<const char*>(_in_ptr);
            const std::size_t len =
                (_max_str_len >= 0) ? strnlen(s, static_cast<std::size_t>(_max_str_len)) : std::strlen(s);
            if (_max_str_len >= 0 && static_cast<int>(len) >= _max_str_len) {
                return USER_PACKSTRUCT_INPUT_ERR;
            }
            _out_sv = std::string_view{s, len};
            if (_max_str_len > 0) {
                _in_ptr = s + _max_str_len;
            }
            else {
                _in_ptr = s + len + 1;
            }
            return 0;
        }

        auto pack_struct_to_lite3_obj(const void*& _in_ptr,
                                      lite3cpp::Buffer& _buf,
                                      std::size_t _obj_ofs,
                                      const packItem_t& _parent_item,
                                      const packInstruct_t* _pack_table,
                                      int _pack_flag,
                                      const char* _pack_instruct_override) -> int;

        auto pack_nonpointer_to_lite3(packItem_t& _item,
                                      std::string_view _key,
                                      const void*& _in_ptr,
                                      lite3cpp::Buffer& _buf,
                                      std::size_t _obj_ofs,
                                      const packInstruct_t* _pack_table,
                                      int _pack_flag) -> int
        {
            const int type_inx = _item.typeInx;
            const int num_element = get_num_element(_item);
            const int element_sz = packTypeTable[type_inx].size;
            const int type_num = packTypeTable[type_inx].number;

            switch (type_num) {
            case PACK_CHAR_TYPE:
            case PACK_BIN_TYPE: {
                const int len = num_element * element_sz;
                if (len <= 0 || !_in_ptr) {
                    _buf.set_bytes(_obj_ofs, _key, {});
                }
                else {
                    _buf.set_bytes(
                        _obj_ofs,
                        _key,
                        {reinterpret_cast<const std::byte*>(_in_ptr), static_cast<std::size_t>(len)});
                    _in_ptr = static_cast<const char*>(_in_ptr) + len;
                }
                return 0;
            }
            case PACK_STR_TYPE:
            case PACK_PI_STR_TYPE: {
                int max_str_len = -1;
                int num_str = 1;
                if (_item.dim > 0) {
                    max_str_len = _item.dimSize[_item.dim - 1];
                    num_str = (max_str_len > 0) ? (num_element / max_str_len) : 0;
                }

                if (num_str == 1 && type_num == PACK_PI_STR_TYPE && _in_ptr) {
                    std::snprintf(_item.strValue, sizeof(_item.strValue), "%s", static_cast<const char*>(_in_ptr));
                }

                if (num_str <= 1) {
                    std::string_view sv;
                    if (num_str == 1) {
                        if (const int ec = read_one_c_str(_in_ptr, max_str_len, sv); ec < 0) {
                            return ec;
                        }
                    }
                    _buf.set_str(_obj_ofs, _key, sv);
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_str; ++i) {
                        std::string_view sv;
                        if (const int ec = read_one_c_str(_in_ptr, max_str_len, sv); ec < 0) {
                            return ec;
                        }
                        _buf.arr_append_str(arr_ofs, sv);
                    }
                }
                return 0;
            }
            case PACK_INT_TYPE: {
                _in_ptr = alignInt(_in_ptr);
                const int* p = static_cast<const int*>(_in_ptr);
                const int first_val = (num_element > 0 && p) ? p[0] : 0;
                _item.intValue = (first_val < 0) ? 0 : first_val;

                if (_item.dim == 0) {
                    _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(first_val));
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(p ? p[i] : 0));
                    }
                }
                if (p) {
                    _in_ptr = p + num_element;
                }
                return 0;
            }
            case PACK_INT16_TYPE: {
                _in_ptr = alignInt16(_in_ptr);
                const short* p = static_cast<const short*>(_in_ptr);
                const short first_val = (num_element > 0 && p) ? p[0] : 0;
                _item.intValue = (first_val < 0) ? 0 : first_val;

                if (_item.dim == 0) {
                    _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(first_val));
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(p ? p[i] : 0));
                    }
                }
                if (p) {
                    _in_ptr = p + num_element;
                }
                return 0;
            }
            case PACK_DOUBLE_TYPE: {
                _in_ptr = alignDouble(_in_ptr);
                const rodsLong_t* p = static_cast<const rodsLong_t*>(_in_ptr);
                const rodsLong_t first_val = (num_element > 0 && p) ? p[0] : 0;

                if (_item.dim == 0) {
                    _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(first_val));
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(p ? p[i] : 0));
                    }
                }
                if (p) {
                    _in_ptr = p + num_element;
                }
                return 0;
            }
            case PACK_STRUCT_TYPE: {
                if (_item.dim == 0) {
                    const std::size_t child_ofs = _buf.set_obj(_obj_ofs, _key);
                    return pack_struct_to_lite3_obj(_in_ptr, _buf, child_ofs, _item, _pack_table, _pack_flag, nullptr);
                }
                const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                for (int i = 0; i < num_element; ++i) {
                    const std::size_t elem_ofs = _buf.arr_append_obj(arr_ofs);
                    if (const int ec =
                            pack_struct_to_lite3_obj(_in_ptr, _buf, elem_ofs, _item, _pack_table, _pack_flag, nullptr);
                        ec < 0)
                    {
                        return ec;
                    }
                }
                return 0;
            }
            default:
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }
        }

        auto pack_pointer_to_lite3(packItem_t& _item,
                                   std::string_view _key,
                                   lite3cpp::Buffer& _buf,
                                   std::size_t _obj_ofs,
                                   const packInstruct_t* _pack_table,
                                   int _pack_flag) -> int
        {
            if (_item.pointer == nullptr) {
                _buf.set_null(_obj_ofs, _key);
                return 0;
            }

            const int num_element = get_num_hint_element(_item);
            const int my_dim = _item.dim;
            const int type_inx = _item.typeInx;
            const int num_pointer = get_num_element(_item);
            const int element_sz = packTypeTable[type_inx].size;
            const int type_num = packTypeTable[type_inx].number;

            const void* singleton_array[1]{_item.pointer};
            const void* const* pointer_array =
                (my_dim == 0) ? singleton_array : static_cast<const void* const*>(_item.pointer);

            if (my_dim == 0 && type_num == PACK_PI_STR_TYPE) {
                std::snprintf(_item.strValue, sizeof(_item.strValue), "%s", static_cast<const char*>(_item.pointer));
            }

            switch (type_num) {
            case PACK_CHAR_TYPE:
            case PACK_BIN_TYPE: {
                const int byte_len = std::max(0, num_element * element_sz);
                if (my_dim == 0) {
                    const void* orig_ptr = pointer_array[0];
                    if (_item.pointerType != NO_PACK_POINTER && orig_ptr && byte_len > 0) {
                        _buf.set_bytes(
                            _obj_ofs,
                            _key,
                            {reinterpret_cast<const std::byte*>(orig_ptr), static_cast<std::size_t>(byte_len)});
                    }
                    else {
                        _buf.set_bytes(_obj_ofs, _key, {});
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<void*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        const void* orig_ptr = pointer_array[i];
                        if (_item.pointerType != NO_PACK_POINTER && orig_ptr) {
                            _buf.arr_append_bytes(
                                arr_ofs,
                                {reinterpret_cast<const std::byte*>(orig_ptr), static_cast<std::size_t>(byte_len)});
                        }
                        else {
                            _buf.arr_append_null(arr_ofs);
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            std::free(const_cast<void*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            case PACK_STR_TYPE:
            case PACK_PI_STR_TYPE: {
                int max_str_len = -1;
                int num_str = 1;
                if (_item.hintDim > 0) {
                    max_str_len = _item.hintDimSize[_item.hintDim - 1];
                    if (num_element <= 0 || max_str_len <= 0) {
                        _buf.set_null(_obj_ofs, _key);
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            if (my_dim == 0) {
                                std::free(const_cast<void*>(pointer_array[0]));
                            }
                            else {
                                for (int i = 0; i < num_pointer; ++i) {
                                    std::free(const_cast<void*>(pointer_array[i]));
                                }
                                if (num_pointer > 0) {
                                    std::free(const_cast<void**>(pointer_array));
                                }
                            }
                        }
                        return 0;
                    }
                    num_str = num_element / max_str_len;
                }

                if (my_dim == 0) {
                    const void* orig_ptr = pointer_array[0];
                    const void* work_ptr = orig_ptr;
                    if (_item.hintDim <= 0) {
                        std::string_view sv;
                        if (const int ec = read_one_c_str(work_ptr, max_str_len, sv); ec < 0) {
                            return ec;
                        }
                        _buf.set_str(_obj_ofs, _key, sv);
                    }
                    else {
                        const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                        for (int i = 0; i < num_str; ++i) {
                            std::string_view sv;
                            if (const int ec = read_one_c_str(work_ptr, max_str_len, sv); ec < 0) {
                                return ec;
                            }
                            _buf.arr_append_str(arr_ofs, sv);
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<void*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int j = 0; j < num_pointer; ++j) {
                        const void* orig_ptr = pointer_array[j];
                        const void* work_ptr = orig_ptr;
                        if (!orig_ptr) {
                            _buf.arr_append_null(arr_ofs);
                        }
                        else if (_item.hintDim <= 0 || num_str == 1) {
                            std::string_view sv;
                            if (const int ec = read_one_c_str(work_ptr, max_str_len, sv); ec < 0) {
                                return ec;
                            }
                            _buf.arr_append_str(arr_ofs, sv);
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_append_arr(arr_ofs);
                            for (int i = 0; i < num_str; ++i) {
                                std::string_view sv;
                                if (const int ec = read_one_c_str(work_ptr, max_str_len, sv); ec < 0) {
                                    return ec;
                                }
                                _buf.arr_append_str(sub_arr, sv);
                            }
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && orig_ptr) {
                            std::free(const_cast<void*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            case PACK_INT_TYPE: {
                if (my_dim == 0) {
                    const int* orig_ptr = static_cast<const int*>(pointer_array[0]);
                    if (_item.hintDim == 0) {
                        _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(orig_ptr ? orig_ptr[0] : 0));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr ? orig_ptr[k] : 0));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<int*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        const int* orig_ptr = static_cast<const int*>(pointer_array[i]);
                        if (!orig_ptr) {
                            _buf.arr_append_null(arr_ofs);
                        }
                        else if (_item.hintDim == 0 && num_element == 1) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr[0]));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_append_arr(arr_ofs);
                            for (int k = 0; k < num_element; ++k) {
                                _buf.arr_append_i64(sub_arr, static_cast<std::int64_t>(orig_ptr[k]));
                            }
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            std::free(const_cast<int*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            case PACK_INT16_TYPE: {
                if (my_dim == 0) {
                    const short* orig_ptr = static_cast<const short*>(pointer_array[0]);
                    if (_item.hintDim == 0) {
                        _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(orig_ptr ? orig_ptr[0] : 0));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr ? orig_ptr[k] : 0));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<short*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        const short* orig_ptr = static_cast<const short*>(pointer_array[i]);
                        if (!orig_ptr) {
                            _buf.arr_append_null(arr_ofs);
                        }
                        else if (_item.hintDim == 0 && num_element == 1) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr[0]));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_append_arr(arr_ofs);
                            for (int k = 0; k < num_element; ++k) {
                                _buf.arr_append_i64(sub_arr, static_cast<std::int64_t>(orig_ptr[k]));
                            }
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            std::free(const_cast<short*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            case PACK_DOUBLE_TYPE: {
                if (my_dim == 0) {
                    const rodsLong_t* orig_ptr = static_cast<const rodsLong_t*>(pointer_array[0]);
                    if (_item.hintDim == 0) {
                        _buf.set_i64(_obj_ofs, _key, static_cast<std::int64_t>(orig_ptr ? orig_ptr[0] : 0));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr ? orig_ptr[k] : 0));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<rodsLong_t*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        const rodsLong_t* orig_ptr = static_cast<const rodsLong_t*>(pointer_array[i]);
                        if (!orig_ptr) {
                            _buf.arr_append_null(arr_ofs);
                        }
                        else if (_item.hintDim == 0 && num_element == 1) {
                            _buf.arr_append_i64(arr_ofs, static_cast<std::int64_t>(orig_ptr[0]));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_append_arr(arr_ofs);
                            for (int k = 0; k < num_element; ++k) {
                                _buf.arr_append_i64(sub_arr, static_cast<std::int64_t>(orig_ptr[k]));
                            }
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            std::free(const_cast<rodsLong_t*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            case PACK_STRUCT_TYPE: {
                if (my_dim == 0) {
                    const void* orig_ptr = pointer_array[0];
                    const void* work_ptr = orig_ptr;
                    if (_item.hintDim == 0 && num_element == 1) {
                        const std::size_t child_ofs = _buf.set_obj(_obj_ofs, _key);
                        const int ec = pack_struct_to_lite3_obj(
                            work_ptr, _buf, child_ofs, _item, _pack_table, _pack_flag, nullptr);
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                            std::free(const_cast<void*>(orig_ptr));
                        }
                        return ec;
                    }
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int k = 0; k < num_element; ++k) {
                        const std::size_t elem_ofs = _buf.arr_append_obj(arr_ofs);
                        const int ec = pack_struct_to_lite3_obj(
                            work_ptr, _buf, elem_ofs, _item, _pack_table, _pack_flag, nullptr);
                        if (ec < 0) {
                            if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                                std::free(const_cast<void*>(orig_ptr));
                            }
                            return ec;
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER) {
                        std::free(const_cast<void*>(orig_ptr));
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.set_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        const void* orig_ptr = pointer_array[i];
                        const void* work_ptr = orig_ptr;
                        if (!orig_ptr) {
                            _buf.arr_append_null(arr_ofs);
                        }
                        else if (num_element == 1) {
                            const std::size_t elem_ofs = _buf.arr_append_obj(arr_ofs);
                            if (const int ec = pack_struct_to_lite3_obj(
                                    work_ptr, _buf, elem_ofs, _item, _pack_table, _pack_flag, nullptr);
                                ec < 0)
                            {
                                return ec;
                            }
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_append_arr(arr_ofs);
                            for (int k = 0; k < num_element; ++k) {
                                const std::size_t elem_ofs = _buf.arr_append_obj(sub_arr);
                                if (const int ec = pack_struct_to_lite3_obj(
                                        work_ptr, _buf, elem_ofs, _item, _pack_table, _pack_flag, nullptr);
                                    ec < 0)
                                {
                                    return ec;
                                }
                            }
                        }
                        if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && orig_ptr) {
                            std::free(const_cast<void*>(orig_ptr));
                        }
                    }
                    if ((_pack_flag & FREE_POINTER) && _item.pointerType == A_POINTER && num_pointer > 0) {
                        std::free(const_cast<void**>(pointer_array));
                    }
                }
                return 0;
            }
            default:
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }
        }

        auto pack_struct_to_lite3_obj(const void*& _in_ptr,
                                      lite3cpp::Buffer& _buf,
                                      std::size_t _obj_ofs,
                                      const packItem_t& _parent_item,
                                      const packInstruct_t* _pack_table,
                                      int _pack_flag,
                                      const char* _pack_instruct_override) -> int
        {
            const char* pi = _pack_instruct_override ? _pack_instruct_override
                                                     : match_pack_instruct(_parent_item.name, _pack_table);
            if (!pi) {
                return SYS_UNMATCH_PACK_INSTRUCTI_NAME;
            }

            packItem_t head{};
            const auto free_head = irods::at_scope_exit{[&head] { free_packed_item_chain(head); }};
            if (const int ec = parse_pack_instruct(pi, head); ec < 0) {
                return ec;
            }
            head.parent = &_parent_item;

            std::unordered_map<std::string, int> field_counts;
            for (packItem_t* cur = &head; cur != nullptr; cur = cur->next) {
                if (const int ec = resolve_packed_item(*cur, _in_ptr, PACK_OPR); ec < 0) {
                    return ec;
                }
                const std::string key = make_field_key(cur->name, field_counts);
                const int ec = (cur->pointerType > 0)
                                   ? pack_pointer_to_lite3(*cur, key, _buf, _obj_ofs, _pack_table, _pack_flag)
                                   : pack_nonpointer_to_lite3(*cur, key, _in_ptr, _buf, _obj_ofs, _pack_table, _pack_flag);
                if (ec < 0) {
                    return ec;
                }
            }

            if (_parent_item.name && std::strcmp(_parent_item.name, "KeyValPair_PI") == 0 &&
                _buf.get_type(_obj_ofs, "ssLen") == lite3cpp::Type::Int64)
            {
                const auto ss_len = _buf.get_i64(_obj_ofs, "ssLen");
                if (ss_len > 0 &&
                    _buf.get_type(_obj_ofs, "keyWord") == lite3cpp::Type::Array &&
                    _buf.get_type(_obj_ofs, "svalue") == lite3cpp::Type::Array)
                {
                    const std::size_t kw_arr = _buf.get_arr(_obj_ofs, "keyWord");
                    const std::size_t val_arr = _buf.get_arr(_obj_ofs, "svalue");
                    const std::size_t kv_ofs = _buf.set_obj(_obj_ofs, "kv");
                    for (std::int64_t i = 0; i < ss_len; ++i) {
                        const auto idx = static_cast<std::uint32_t>(i);
                        if (_buf.arr_get_type(kw_arr, idx) == lite3cpp::Type::String) {
                            const std::string key_str{_buf.arr_get_str(kw_arr, idx)};
                            const std::string val_str =
                                (_buf.arr_get_type(val_arr, idx) == lite3cpp::Type::String)
                                    ? std::string{_buf.arr_get_str(val_arr, idx)}
                                    : std::string{};
                            _buf.set_str(kv_ofs, key_str, val_str);
                        }
                    }
                }
            }

            return 0;
        }

        auto init_packed_output(int _len) -> packedOutput_t
        {
            packedOutput_t out{};
            out.bBuf.len = 0;
            out.bBuf.buf = std::calloc(1, static_cast<std::size_t>(_len));
            out.bufSize = _len;
            return out;
        }

        auto extend_packed_output(packedOutput_t& _packed_output, int _ext_len, void*& _out_ptr) -> int
        {
            const int new_out_len = _packed_output.bBuf.len + _ext_len;
            if (new_out_len <= _packed_output.bufSize) {
                _out_ptr = static_cast<char*>(_packed_output.bBuf.buf) + _packed_output.bBuf.len;
                return 0;
            }

            int new_buf_size = _packed_output.bufSize + _packed_output.bufSize;
            if (new_buf_size <= new_out_len || _packed_output.bufSize > MAX_PACKED_OUT_ALLOC_SZ) {
                new_buf_size = new_out_len + PACKED_OUT_ALLOC_SZ;
            }

            void* tmp = std::realloc(_packed_output.bBuf.buf, static_cast<std::size_t>(new_buf_size));
            if (!tmp) {
                _out_ptr = nullptr;
                return SYS_MALLOC_ERR;
            }
            _packed_output.bBuf.buf = tmp;
            _packed_output.bufSize = new_buf_size;
            _out_ptr = static_cast<char*>(_packed_output.bBuf.buf) + _packed_output.bBuf.len;
            std::memset(_out_ptr, 0, static_cast<std::size_t>(new_buf_size - _packed_output.bBuf.len));
            return 0;
        }

        auto add_pointer_to_packed_out(packedOutput_t& _packed_output, int _len, void* _pointer) -> void*
        {
            void* out_ptr = nullptr;
            extend_packed_output(_packed_output, sizeof(void*), out_ptr);
            out_ptr = ialignAddr(out_ptr);

            void** tmp_ptr = static_cast<void**>(out_ptr);
            if (_pointer != nullptr) {
                *tmp_ptr = _pointer;
            }
            else if (_len > 0) {
                *tmp_ptr = std::calloc(1, static_cast<std::size_t>(_len));
            }
            else {
                *tmp_ptr = nullptr;
            }

            _packed_output.bBuf.len =
                static_cast<int>(static_cast<char*>(out_ptr) - static_cast<char*>(_packed_output.bBuf.buf)) +
                static_cast<int>(sizeof(void*));
            return *tmp_ptr;
        }

        auto decode_hex_bytes(std::string_view _hex) -> std::vector<std::byte>
        {
            std::vector<std::byte> out;
            out.reserve(_hex.size() / 2);
            for (std::size_t i = 0; i + 1 < _hex.size(); i += 2) {
                unsigned int byte_val = 0;
                std::from_chars(_hex.data() + i, _hex.data() + i + 2, byte_val, 16);
                out.push_back(static_cast<std::byte>(byte_val));
            }
            return out;
        }

        void copy_bytes_from_lite3_key(const lite3cpp::Buffer& _buf,
                                       std::size_t _obj_ofs,
                                       std::string_view _key,
                                       void* _dest,
                                       int _len)
        {
            if (!_dest || _len <= 0) {
                return;
            }
            std::memset(_dest, 0, static_cast<std::size_t>(_len));
            const auto t = _buf.get_type(_obj_ofs, _key);
            if (t == lite3cpp::Type::Bytes) {
                const auto span = _buf.get_bytes(_obj_ofs, _key);
                const std::size_t n = std::min(span.size(), static_cast<std::size_t>(_len));
                if (n > 0) {
                    std::memcpy(_dest, span.data(), n);
                }
            }
            else if (t == lite3cpp::Type::String) {
                const auto decoded = decode_hex_bytes(_buf.get_str(_obj_ofs, _key));
                const std::size_t n = std::min(decoded.size(), static_cast<std::size_t>(_len));
                if (n > 0) {
                    std::memcpy(_dest, decoded.data(), n);
                }
            }
        }

        void copy_bytes_from_lite3_arr(const lite3cpp::Buffer& _buf,
                                       std::size_t _arr_ofs,
                                       std::uint32_t _idx,
                                       void* _dest,
                                       int _len)
        {
            if (!_dest || _len <= 0) {
                return;
            }
            std::memset(_dest, 0, static_cast<std::size_t>(_len));
            const auto t = _buf.arr_get_type(_arr_ofs, _idx);
            if (t == lite3cpp::Type::Bytes) {
                const auto span = _buf.arr_get_bytes(_arr_ofs, _idx);
                const std::size_t n = std::min(span.size(), static_cast<std::size_t>(_len));
                if (n > 0) {
                    std::memcpy(_dest, span.data(), n);
                }
            }
            else if (t == lite3cpp::Type::String) {
                const auto decoded = decode_hex_bytes(_buf.arr_get_str(_arr_ofs, _idx));
                const std::size_t n = std::min(decoded.size(), static_cast<std::size_t>(_len));
                if (n > 0) {
                    std::memcpy(_dest, decoded.data(), n);
                }
            }
        }

        auto unpack_lite3_obj_to_struct(const lite3cpp::Buffer& _buf,
                                        std::size_t _obj_ofs,
                                        packedOutput_t& _unpacked_output,
                                        const packItem_t& _parent_item,
                                        const packInstruct_t* _pack_table,
                                        const char* _pack_instruct_override) -> int;

        auto unpack_nonpointer_from_lite3(packItem_t& _item,
                                          std::string_view _key,
                                          const lite3cpp::Buffer& _buf,
                                          std::size_t _obj_ofs,
                                          packedOutput_t& _unpacked_output,
                                          const packInstruct_t* _pack_table) -> int
        {
            const int type_inx = _item.typeInx;
            const int num_element = get_num_element(_item);
            const int element_sz = packTypeTable[type_inx].size;
            const int type_num = packTypeTable[type_inx].number;

            switch (type_num) {
            case PACK_CHAR_TYPE:
            case PACK_BIN_TYPE: {
                const int len = num_element * element_sz;
                if (len <= 0) {
                    return 0;
                }
                void* out_ptr = nullptr;
                if (const int ec = extend_packed_output(_unpacked_output, len, out_ptr); ec < 0) {
                    return ec;
                }
                copy_bytes_from_lite3_key(_buf, _obj_ofs, _key, out_ptr, len);
                _unpacked_output.bBuf.len += len;
                return 0;
            }
            case PACK_STR_TYPE:
            case PACK_PI_STR_TYPE: {
                int max_str_len = -1;
                int num_str = 1;
                if (_item.dim > 0) {
                    max_str_len = _item.dimSize[_item.dim - 1];
                    num_str = (max_str_len > 0) ? (num_element / max_str_len) : 0;
                }

                if (num_str <= 1) {
                    const std::string_view sv =
                        (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::String) ? _buf.get_str(_obj_ofs, _key) : "";
                    if (max_str_len >= 0 && static_cast<int>(sv.size()) >= max_str_len) {
                        return USER_PACKSTRUCT_INPUT_ERR;
                    }
                    const int ext_len = (max_str_len >= 0) ? max_str_len : (static_cast<int>(sv.size()) + 1);
                    void* out_ptr = nullptr;
                    if (const int ec = extend_packed_output(_unpacked_output, ext_len, out_ptr); ec < 0) {
                        return ec;
                    }
                    std::memset(out_ptr, 0, static_cast<std::size_t>(ext_len));
                    if (!sv.empty()) {
                        std::memcpy(out_ptr, sv.data(), sv.size());
                    }
                    _unpacked_output.bBuf.len += ext_len;
                    if (type_num == PACK_PI_STR_TYPE) {
                        rstrcpy(_item.strValue, static_cast<const char*>(out_ptr), NAME_LEN);
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_str; ++i) {
                        const std::string_view sv = _buf.arr_get_str(arr_ofs, static_cast<std::uint32_t>(i));
                        if (max_str_len >= 0 && static_cast<int>(sv.size()) >= max_str_len) {
                            return USER_PACKSTRUCT_INPUT_ERR;
                        }
                        const int ext_len = (max_str_len >= 0) ? max_str_len : (static_cast<int>(sv.size()) + 1);
                        void* out_ptr = nullptr;
                        if (const int ec = extend_packed_output(_unpacked_output, ext_len, out_ptr); ec < 0) {
                            return ec;
                        }
                        std::memset(out_ptr, 0, static_cast<std::size_t>(ext_len));
                        if (!sv.empty()) {
                            std::memcpy(out_ptr, sv.data(), sv.size());
                        }
                        _unpacked_output.bBuf.len += ext_len;
                        if (type_num == PACK_PI_STR_TYPE && i == 0) {
                            rstrcpy(_item.strValue, static_cast<const char*>(out_ptr), NAME_LEN);
                        }
                    }
                }
                return 0;
            }
            case PACK_INT_TYPE: {
                if (num_element == 0) {
                    return 0;
                }
                void* out_ptr = nullptr;
                if (const int ec =
                        extend_packed_output(_unpacked_output, static_cast<int>(sizeof(int)) * (num_element + 1), out_ptr);
                    ec < 0)
                {
                    return ec;
                }
                out_ptr = alignInt(out_ptr);
                int* dst = static_cast<int*>(out_ptr);
                if (_item.dim == 0) {
                    dst[0] = (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Int64)
                                 ? static_cast<int>(_buf.get_i64(_obj_ofs, _key))
                                 : 0;
                }
                else if (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Array) {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        dst[i] = static_cast<int>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                    }
                }
                else {
                    std::memset(dst, 0, sizeof(int) * static_cast<std::size_t>(num_element));
                }
                _unpacked_output.bBuf.len =
                    static_cast<int>(static_cast<char*>(out_ptr) - static_cast<char*>(_unpacked_output.bBuf.buf)) +
                    static_cast<int>(sizeof(int)) * num_element;
                _item.intValue = (dst[0] < 0) ? 0 : dst[0];
                return 0;
            }
            case PACK_INT16_TYPE: {
                if (num_element == 0) {
                    return 0;
                }
                void* out_ptr = nullptr;
                if (const int ec = extend_packed_output(
                        _unpacked_output, static_cast<int>(sizeof(short)) * (num_element + 1), out_ptr);
                    ec < 0)
                {
                    return ec;
                }
                out_ptr = alignInt16(out_ptr);
                short* dst = static_cast<short*>(out_ptr);
                if (_item.dim == 0) {
                    dst[0] = (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Int64)
                                 ? static_cast<short>(_buf.get_i64(_obj_ofs, _key))
                                 : 0;
                }
                else if (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Array) {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        dst[i] = static_cast<short>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                    }
                }
                else {
                    std::memset(dst, 0, sizeof(short) * static_cast<std::size_t>(num_element));
                }
                _unpacked_output.bBuf.len =
                    static_cast<int>(static_cast<char*>(out_ptr) - static_cast<char*>(_unpacked_output.bBuf.buf)) +
                    static_cast<int>(sizeof(short)) * num_element;
                _item.intValue = (dst[0] < 0) ? 0 : dst[0];
                return 0;
            }
            case PACK_DOUBLE_TYPE: {
                if (num_element == 0) {
                    return 0;
                }
                void* out_ptr = nullptr;
                if (const int ec = extend_packed_output(
                        _unpacked_output, static_cast<int>(sizeof(rodsLong_t)) * (num_element + 1), out_ptr);
                    ec < 0)
                {
                    return ec;
                }
                out_ptr = alignDouble(out_ptr);
                rodsLong_t* dst = static_cast<rodsLong_t*>(out_ptr);
                if (_item.dim == 0) {
                    dst[0] = (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Int64)
                                 ? static_cast<rodsLong_t>(_buf.get_i64(_obj_ofs, _key))
                                 : 0;
                }
                else if (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Array) {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_element; ++i) {
                        dst[i] = static_cast<rodsLong_t>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                    }
                }
                else {
                    std::memset(dst, 0, sizeof(rodsLong_t) * static_cast<std::size_t>(num_element));
                }
                _unpacked_output.bBuf.len =
                    static_cast<int>(static_cast<char*>(out_ptr) - static_cast<char*>(_unpacked_output.bBuf.buf)) +
                    static_cast<int>(sizeof(rodsLong_t)) * num_element;
                return 0;
            }
            case PACK_STRUCT_TYPE: {
                if (_item.dim == 0) {
                    if (_buf.get_type(_obj_ofs, _key) == lite3cpp::Type::Object) {
                        const std::size_t child_ofs = _buf.get_obj(_obj_ofs, _key);
                        return unpack_lite3_obj_to_struct(_buf, child_ofs, _unpacked_output, _item, _pack_table, nullptr);
                    }
                    lite3cpp::Buffer empty_buf;
                    empty_buf.init_object();
                    return unpack_lite3_obj_to_struct(empty_buf, 0, _unpacked_output, _item, _pack_table, nullptr);
                }
                const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                for (int i = 0; i < num_element; ++i) {
                    const std::size_t elem_ofs = _buf.arr_get_obj(arr_ofs, static_cast<std::uint32_t>(i));
                    if (const int ec =
                            unpack_lite3_obj_to_struct(_buf, elem_ofs, _unpacked_output, _item, _pack_table, nullptr);
                        ec < 0)
                    {
                        return ec;
                    }
                }
                return 0;
            }
            default:
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }
        }

        auto unpack_pointer_from_lite3(packItem_t& _item,
                                       std::string_view _key,
                                       const lite3cpp::Buffer& _buf,
                                       std::size_t _obj_ofs,
                                       packedOutput_t& _unpacked_output,
                                       const packInstruct_t* _pack_table) -> int
        {
            const auto key_type = _buf.get_type(_obj_ofs, _key);
            if (key_type == lite3cpp::Type::Null || key_type == lite3cpp::Type::Invalid) {
                add_pointer_to_packed_out(_unpacked_output, 0, nullptr);
                return 0;
            }

            const int my_dim = _item.dim;
            const int type_inx = _item.typeInx;
            const int num_pointer = get_num_element(_item);
            const int num_element = get_num_hint_element(_item);
            const int element_sz = packTypeTable[type_inx].size;
            const int type_num = packTypeTable[type_inx].number;

            if (num_element <= 0 || (my_dim > 0 && num_pointer <= 0)) {
                add_pointer_to_packed_out(_unpacked_output, 0, nullptr);
                return 0;
            }

            if (my_dim < 0) {
                return SYS_NEGATIVE_SIZE;
            }

            void** pointer_array = nullptr;
            if (my_dim > 0) {
                const int my_modu = num_pointer % PTR_ARRAY_MALLOC_LEN;
                const int alloc_len =
                    (my_modu == 0) ? num_pointer : (num_pointer + PTR_ARRAY_MALLOC_LEN - my_modu);
                if (type_num == PACK_DOUBLE_TYPE || type_num == PACK_INT_TYPE || type_num == PACK_INT16_TYPE) {
                    pointer_array = static_cast<void**>(
                        add_pointer_to_packed_out(_unpacked_output, alloc_len * element_sz, nullptr));
                }
                else {
                    pointer_array = static_cast<void**>(
                        add_pointer_to_packed_out(_unpacked_output, alloc_len * static_cast<int>(sizeof(void*)), nullptr));
                }
            }

            switch (type_num) {
            case PACK_CHAR_TYPE:
            case PACK_BIN_TYPE: {
                const int len = num_element * element_sz;
                if (my_dim == 0) {
                    if (_item.pointerType != NO_PACK_POINTER) {
                        void* out_ptr = add_pointer_to_packed_out(_unpacked_output, len, nullptr);
                        copy_bytes_from_lite3_key(_buf, _obj_ofs, _key, out_ptr, len);
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        if (_item.pointerType != NO_PACK_POINTER) {
                            if (_buf.arr_get_type(arr_ofs, static_cast<std::uint32_t>(i)) == lite3cpp::Type::Null) {
                                pointer_array[i] = nullptr;
                            }
                            else {
                                pointer_array[i] = std::calloc(1, static_cast<std::size_t>(len));
                                copy_bytes_from_lite3_arr(
                                    _buf, arr_ofs, static_cast<std::uint32_t>(i), pointer_array[i], len);
                            }
                        }
                    }
                }
                return 0;
            }
            case PACK_STR_TYPE:
            case PACK_PI_STR_TYPE: {
                int max_str_len = 0;
                int num_str = 0;
                get_num_str_and_str_len(_item, num_str, max_str_len);
                if (max_str_len == 0) {
                    add_pointer_to_packed_out(_unpacked_output, 0, nullptr);
                    return 0;
                }

                if (my_dim == 0) {
                    if (num_str <= 1) {
                        std::string_view sv;
                        if (key_type == lite3cpp::Type::String) {
                            sv = _buf.get_str(_obj_ofs, _key);
                        }
                        else if (key_type == lite3cpp::Type::Array && num_str == 1) {
                            const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                            sv = _buf.arr_get_str(arr_ofs, 0);
                        }
                        if (max_str_len >= 0 && static_cast<int>(sv.size()) >= max_str_len) {
                            return USER_PACKSTRUCT_INPUT_ERR;
                        }
                        const int my_len = (max_str_len > 0) ? max_str_len : (static_cast<int>(sv.size()) + 1);
                        char* out_str = static_cast<char*>(add_pointer_to_packed_out(_unpacked_output, my_len, nullptr));
                        if (num_str == 1 && out_str) {
                            if (!sv.empty()) {
                                std::memcpy(out_str, sv.data(), sv.size());
                            }
                            out_str[sv.size()] = '\0';
                            if (type_num == PACK_PI_STR_TYPE) {
                                rstrcpy(_item.strValue, out_str, NAME_LEN);
                            }
                        }
                    }
                    else {
                        if (max_str_len < 0) {
                            return SYS_PACK_INSTRUCT_FORMAT_ERR;
                        }
                        const int my_len = num_str * max_str_len;
                        char* out_str = static_cast<char*>(add_pointer_to_packed_out(_unpacked_output, my_len, nullptr));
                        const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                        for (int i = 0; i < num_str; ++i) {
                            const std::string_view sv = _buf.arr_get_str(arr_ofs, static_cast<std::uint32_t>(i));
                            if (static_cast<int>(sv.size()) >= max_str_len) {
                                return USER_PACKSTRUCT_INPUT_ERR;
                            }
                            char* slot = out_str + i * max_str_len;
                            if (!sv.empty()) {
                                std::memcpy(slot, sv.data(), sv.size());
                            }
                            slot[sv.size()] = '\0';
                            if (type_num == PACK_PI_STR_TYPE && i == 0) {
                                rstrcpy(_item.strValue, slot, NAME_LEN);
                            }
                        }
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int j = 0; j < num_pointer; ++j) {
                        const auto elem_type = _buf.arr_get_type(arr_ofs, static_cast<std::uint32_t>(j));
                        if (elem_type == lite3cpp::Type::Null) {
                            pointer_array[j] = nullptr;
                            continue;
                        }
                        if (num_str <= 1) {
                            const std::string_view sv = _buf.arr_get_str(arr_ofs, static_cast<std::uint32_t>(j));
                            if (max_str_len >= 0 && static_cast<int>(sv.size()) >= max_str_len) {
                                return USER_PACKSTRUCT_INPUT_ERR;
                            }
                            const int my_len = (max_str_len > 0) ? max_str_len : (static_cast<int>(sv.size()) + 1);
                            char* slot = static_cast<char*>(std::calloc(1, static_cast<std::size_t>(my_len)));
                            if (!sv.empty()) {
                                std::memcpy(slot, sv.data(), sv.size());
                            }
                            slot[sv.size()] = '\0';
                            pointer_array[j] = slot;
                        }
                        else {
                            if (max_str_len < 0) {
                                return SYS_PACK_INSTRUCT_FORMAT_ERR;
                            }
                            const int my_len = num_str * max_str_len;
                            char* base_slot = static_cast<char*>(std::calloc(1, static_cast<std::size_t>(my_len)));
                            const std::size_t sub_arr = _buf.arr_get_arr(arr_ofs, static_cast<std::uint32_t>(j));
                            for (int i = 0; i < num_str; ++i) {
                                const std::string_view sv = _buf.arr_get_str(sub_arr, static_cast<std::uint32_t>(i));
                                if (static_cast<int>(sv.size()) >= max_str_len) {
                                    std::free(base_slot);
                                    return USER_PACKSTRUCT_INPUT_ERR;
                                }
                                char* slot = base_slot + i * max_str_len;
                                if (!sv.empty()) {
                                    std::memcpy(slot, sv.data(), sv.size());
                                }
                                slot[sv.size()] = '\0';
                            }
                            pointer_array[j] = base_slot;
                        }
                    }
                }
                return 0;
            }
            case PACK_INT_TYPE: {
                if (my_dim == 0) {
                    int* dst =
                        static_cast<int*>(add_pointer_to_packed_out(_unpacked_output, num_element * element_sz, nullptr));
                    if (_item.hintDim == 0 && key_type == lite3cpp::Type::Int64) {
                        dst[0] = static_cast<int>(_buf.get_i64(_obj_ofs, _key));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            dst[k] = static_cast<int>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(k)));
                        }
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        int* dst = static_cast<int*>(std::calloc(static_cast<std::size_t>(num_element), sizeof(int)));
                        pointer_array[i] = dst;
                        if (_item.hintDim == 0 && num_element == 1) {
                            dst[0] = static_cast<int>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_get_arr(arr_ofs, static_cast<std::uint32_t>(i));
                            for (int k = 0; k < num_element; ++k) {
                                dst[k] = static_cast<int>(_buf.arr_get_i64(sub_arr, static_cast<std::uint32_t>(k)));
                            }
                        }
                    }
                }
                return 0;
            }
            case PACK_INT16_TYPE: {
                if (my_dim == 0) {
                    short* dst = static_cast<short*>(
                        add_pointer_to_packed_out(_unpacked_output, num_element * element_sz, nullptr));
                    if (_item.hintDim == 0 && key_type == lite3cpp::Type::Int64) {
                        dst[0] = static_cast<short>(_buf.get_i64(_obj_ofs, _key));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            dst[k] = static_cast<short>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(k)));
                        }
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        short* dst =
                            static_cast<short*>(std::calloc(static_cast<std::size_t>(num_element), sizeof(short)));
                        pointer_array[i] = dst;
                        if (_item.hintDim == 0 && num_element == 1) {
                            dst[0] = static_cast<short>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_get_arr(arr_ofs, static_cast<std::uint32_t>(i));
                            for (int k = 0; k < num_element; ++k) {
                                dst[k] = static_cast<short>(_buf.arr_get_i64(sub_arr, static_cast<std::uint32_t>(k)));
                            }
                        }
                    }
                }
                return 0;
            }
            case PACK_DOUBLE_TYPE: {
                if (my_dim == 0) {
                    rodsLong_t* dst = static_cast<rodsLong_t*>(
                        add_pointer_to_packed_out(_unpacked_output, num_element * element_sz, nullptr));
                    if (_item.hintDim == 0 && key_type == lite3cpp::Type::Int64) {
                        dst[0] = static_cast<rodsLong_t>(_buf.get_i64(_obj_ofs, _key));
                    }
                    else {
                        const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            dst[k] = static_cast<rodsLong_t>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(k)));
                        }
                    }
                }
                else {
                    const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                    for (int i = 0; i < num_pointer; ++i) {
                        rodsLong_t* dst = static_cast<rodsLong_t*>(
                            std::calloc(static_cast<std::size_t>(num_element), sizeof(rodsLong_t)));
                        pointer_array[i] = dst;
                        if (_item.hintDim == 0 && num_element == 1) {
                            dst[0] = static_cast<rodsLong_t>(_buf.arr_get_i64(arr_ofs, static_cast<std::uint32_t>(i)));
                        }
                        else {
                            const std::size_t sub_arr = _buf.arr_get_arr(arr_ofs, static_cast<std::uint32_t>(i));
                            for (int k = 0; k < num_element; ++k) {
                                dst[k] =
                                    static_cast<rodsLong_t>(_buf.arr_get_i64(sub_arr, static_cast<std::uint32_t>(k)));
                            }
                        }
                    }
                }
                return 0;
            }
            case PACK_STRUCT_TYPE: {
                if (my_dim == 0) {
                    packedOutput_t sub_out = init_packed_output(num_element * SUB_STRUCT_ALLOC_SZ);
                    int status = 0;
                    if (_item.hintDim == 0 && num_element == 1 && key_type == lite3cpp::Type::Object) {
                        const std::size_t child_ofs = _buf.get_obj(_obj_ofs, _key);
                        status = unpack_lite3_obj_to_struct(_buf, child_ofs, sub_out, _item, _pack_table, nullptr);
                    }
                    else {
                        const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                        for (int k = 0; k < num_element; ++k) {
                            const std::size_t elem_ofs = _buf.arr_get_obj(arr_ofs, static_cast<std::uint32_t>(k));
                            status = unpack_lite3_obj_to_struct(_buf, elem_ofs, sub_out, _item, _pack_table, nullptr);
                            if (status < 0) {
                                break;
                            }
                        }
                    }
                    add_pointer_to_packed_out(_unpacked_output, num_element * SUB_STRUCT_ALLOC_SZ, sub_out.bBuf.buf);
                    return status;
                }
                const std::size_t arr_ofs = _buf.get_arr(_obj_ofs, _key);
                for (int i = 0; i < num_pointer; ++i) {
                    const auto elem_type = _buf.arr_get_type(arr_ofs, static_cast<std::uint32_t>(i));
                    if (elem_type == lite3cpp::Type::Null) {
                        pointer_array[i] = nullptr;
                        continue;
                    }
                    packedOutput_t sub_out = init_packed_output(num_element * SUB_STRUCT_ALLOC_SZ);
                    int status = 0;
                    if (num_element == 1) {
                        const std::size_t elem_ofs = _buf.arr_get_obj(arr_ofs, static_cast<std::uint32_t>(i));
                        status = unpack_lite3_obj_to_struct(_buf, elem_ofs, sub_out, _item, _pack_table, nullptr);
                    }
                    else {
                        const std::size_t sub_arr = _buf.arr_get_arr(arr_ofs, static_cast<std::uint32_t>(i));
                        for (int k = 0; k < num_element; ++k) {
                            const std::size_t elem_ofs = _buf.arr_get_obj(sub_arr, static_cast<std::uint32_t>(k));
                            status = unpack_lite3_obj_to_struct(_buf, elem_ofs, sub_out, _item, _pack_table, nullptr);
                            if (status < 0) {
                                break;
                            }
                        }
                    }
                    pointer_array[i] = sub_out.bBuf.buf;
                    if (status < 0) {
                        return status;
                    }
                }
                return 0;
            }
            default:
                return SYS_PACK_INSTRUCT_FORMAT_ERR;
            }
        }

        auto unpack_lite3_obj_to_struct(const lite3cpp::Buffer& _buf,
                                        std::size_t _obj_ofs,
                                        packedOutput_t& _unpacked_output,
                                        const packItem_t& _parent_item,
                                        const packInstruct_t* _pack_table,
                                        const char* _pack_instruct_override) -> int
        {
            if (_parent_item.name && std::strcmp(_parent_item.name, "KeyValPair_PI") == 0 &&
                _buf.get_type(_obj_ofs, "kv") == lite3cpp::Type::Object)
            {
                const std::size_t kv_ofs = _buf.get_obj(_obj_ofs, "kv");
                std::vector<std::pair<std::string_view, std::string_view>> entries;
                for (auto it = _buf.begin(kv_ofs); it != _buf.end(kv_ofs); ++it) {
                    if (it->value_type == lite3cpp::Type::String) {
                        entries.emplace_back(it->key, _buf.get_str(kv_ofs, it->key));
                    }
                }

                void* out_ptr = nullptr;
                if (const int ec = extend_packed_output(_unpacked_output, static_cast<int>(sizeof(int)) * 2, out_ptr);
                    ec < 0)
                {
                    return ec;
                }
                out_ptr = alignInt(out_ptr);
                const int n = static_cast<int>(entries.size());
                *static_cast<int*>(out_ptr) = n;
                _unpacked_output.bBuf.len =
                    static_cast<int>(static_cast<char*>(out_ptr) - static_cast<char*>(_unpacked_output.bBuf.buf)) +
                    static_cast<int>(sizeof(int));

                if (n == 0) {
                    add_pointer_to_packed_out(_unpacked_output, 0, nullptr);
                    add_pointer_to_packed_out(_unpacked_output, 0, nullptr);
                }
                else {
                    const int my_modu = n % PTR_ARRAY_MALLOC_LEN;
                    const int alloc_len = (my_modu == 0) ? n : (n + PTR_ARRAY_MALLOC_LEN - my_modu);
                    auto** kw_arr = static_cast<char**>(add_pointer_to_packed_out(
                        _unpacked_output, alloc_len * static_cast<int>(sizeof(void*)), nullptr));
                    for (int i = 0; i < n; ++i) {
                        kw_arr[i] = strdup(std::string{entries[i].first}.c_str());
                    }
                    auto** val_arr = static_cast<char**>(add_pointer_to_packed_out(
                        _unpacked_output, alloc_len * static_cast<int>(sizeof(void*)), nullptr));
                    for (int i = 0; i < n; ++i) {
                        val_arr[i] = strdup(std::string{entries[i].second}.c_str());
                    }
                }
                return 0;
            }

            const char* pi = _pack_instruct_override ? _pack_instruct_override
                                                     : match_pack_instruct(_parent_item.name, _pack_table);
            if (!pi) {
                return SYS_UNMATCH_PACK_INSTRUCTI_NAME;
            }

            packItem_t head{};
            const auto free_head = irods::at_scope_exit{[&head] { free_packed_item_chain(head); }};
            if (const int ec = parse_pack_instruct(pi, head); ec < 0) {
                return ec;
            }
            head.parent = &_parent_item;

            std::unordered_map<std::string, int> field_counts;
            for (packItem_t* cur = &head; cur != nullptr; cur = cur->next) {
                const void* dummy_ptr = nullptr;
                if (const int ec = resolve_packed_item(*cur, dummy_ptr, UNPACK_OPR); ec < 0) {
                    return ec;
                }
                const std::string key = make_field_key(cur->name, field_counts);
                const int ec = (cur->pointerType > 0)
                                   ? unpack_pointer_from_lite3(*cur, key, _buf, _obj_ofs, _unpacked_output, _pack_table)
                                   : unpack_nonpointer_from_lite3(*cur, key, _buf, _obj_ofs, _unpacked_output, _pack_table);
                if (ec < 0) {
                    return ec;
                }
            }
            return 0;
        }

        auto compute_lite3_node_end(const std::uint8_t* _base,
                                    std::size_t _node_ofs,
                                    std::size_t _max_bound,
                                    int _depth) noexcept -> std::size_t
        {
            if (!_base || _depth > 32 || _node_ofs + lite3cpp::config::node_size > _max_bound) {
                return 0;
            }

            lite3cpp::NodeView node(reinterpret_cast<const lite3cpp::PackedNodeLayout*>(_base + _node_ofs));
            const auto node_type = node.type();
            if (node_type != lite3cpp::Type::Object && node_type != lite3cpp::Type::Array) {
                return 0;
            }

            const std::uint32_t kc = node.key_count();
            if (kc > lite3cpp::config::node_key_count) {
                return 0;
            }

            std::size_t max_end = _node_ofs + lite3cpp::config::node_size;
            const bool is_arr = (node_type == lite3cpp::Type::Array);

            for (std::uint32_t i = 0; i < kc; ++i) {
                const std::size_t kv_ofs = node.get_kv_offset(i);
                if (kv_ofs >= _max_bound) {
                    return max_end;
                }
                std::size_t vo = kv_ofs;
                if (!is_arr) {
                    const std::uint8_t tag = _base[vo];
                    const std::uint32_t klen = (tag >> 2);
                    vo += 1 + klen;
                }
                if (vo >= _max_bound) {
                    return max_end;
                }
                const auto val_type = static_cast<lite3cpp::Type>(_base[vo]);
                switch (val_type) {
                case lite3cpp::Type::Null:
                    max_end = std::max(max_end, vo + 1);
                    break;
                case lite3cpp::Type::Bool:
                    max_end = std::max(max_end, vo + 2);
                    break;
                case lite3cpp::Type::Int64:
                case lite3cpp::Type::Float64:
                    max_end = std::max(max_end, vo + 9);
                    break;
                case lite3cpp::Type::Bytes: {
                    if (vo + 5 <= _max_bound) {
                        std::uint32_t sz = 0;
                        std::memcpy(&sz, _base + vo + 1, sizeof(std::uint32_t));
                        if (static_cast<std::size_t>(sz) <= _max_bound - (vo + 5)) {
                            max_end = std::max(max_end, vo + 5 + static_cast<std::size_t>(sz));
                        }
                        else {
                            max_end = _max_bound;
                        }
                    }
                    break;
                }
                case lite3cpp::Type::String: {
                    if (vo + 6 <= _max_bound) {
                        std::uint32_t sz = 0;
                        std::memcpy(&sz, _base + vo + 1, sizeof(std::uint32_t));
                        if (static_cast<std::size_t>(sz) <= _max_bound - (vo + 6)) {
                            max_end = std::max(max_end, vo + 6 + static_cast<std::size_t>(sz));
                        }
                        else {
                            max_end = _max_bound;
                        }
                    }
                    break;
                }
                case lite3cpp::Type::Object:
                case lite3cpp::Type::Array: {
                    max_end = std::max(max_end, compute_lite3_node_end(_base, vo + 1, _max_bound, _depth + 1));
                    break;
                }
                default:
                    break;
                }
            }

            if (node.get_child_offset(0) != 0) {
                for (std::uint32_t c = 0; c <= kc; ++c) {
                    const std::size_t child_ofs = node.get_child_offset(c);
                    if (child_ofs != 0) {
                        max_end = std::max(max_end, compute_lite3_node_end(_base, child_ofs, _max_bound, _depth + 1));
                    }
                }
            }

            return max_end;
        }
    } // anonymous namespace

    auto negotiate_protocol(std::string_view _client_rel_version,
                            std::string_view _server_rel_version,
                            irodsProt_t _requested_prot,
                            bool _explicit_env_override) noexcept -> irodsProt_t
    {
        const auto cli_ver = irods::to_version(std::string{_client_rel_version});
        const auto svr_ver = irods::to_version(std::string{_server_rel_version});
        const bool both_v6_or_later =
            cli_ver && svr_ver && (*cli_ver >= irods::version{6, 0, 0}) && (*svr_ver >= irods::version{6, 0, 0});

        if (_requested_prot == LITE3_PROT) {
            return both_v6_or_later ? LITE3_PROT : NATIVE_PROT;
        }

        if (_explicit_env_override || _requested_prot == XML_PROT || _requested_prot == JSON_PROT) {
            return _requested_prot;
        }

        if (both_v6_or_later) {
            return LITE3_PROT;
        }

        return _requested_prot;
    }

    auto compute_lite3_buffer_size(const std::uint8_t* _base, std::size_t _max_bound) noexcept -> std::size_t
    {
        return compute_lite3_node_end(_base, 0, _max_bound, 0);
    }

    auto struct_to_lite3(const void* _in_struct,
                         std::string_view _pack_instruct_name,
                         const packInstruct_t* _pack_table,
                         int _pack_flag) -> expected<lite3cpp::Buffer, int>
    {
        if (!_in_struct || _pack_instruct_name.empty()) {
            return unexpected{USER_PACKSTRUCT_INPUT_ERR};
        }

        try {
            lite3cpp::Buffer buf;
            buf.init_object();

            std::string pi_name{_pack_instruct_name};
            packItem_t root_item{};
            root_item.name = strdup(pi_name.c_str());
            const auto free_root = irods::at_scope_exit{[&root_item] { std::free(root_item.name); }};

            const void* in_ptr = _in_struct;
            const int status = pack_struct_to_lite3_obj(in_ptr, buf, 0, root_item, _pack_table, _pack_flag, nullptr);
            if (status < 0) {
                return unexpected{status};
            }
            return buf;
        }
        catch (const lite3cpp::exception&) {
            return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
        }
        catch (const std::bad_alloc&) {
            return unexpected{SYS_MALLOC_ERR};
        }
        catch (...) {
            return unexpected{SYS_INTERNAL_ERR};
        }
    }

    auto lite3_to_struct(const lite3cpp::Buffer& _buf,
                         void** _out_struct,
                         std::string_view _pack_instruct_name,
                         const packInstruct_t* _pack_table) -> int
    {
        if (!_out_struct || _pack_instruct_name.empty() || _buf.size() < lite3cpp::config::node_size) {
            return USER_PACKSTRUCT_INPUT_ERR;
        }

        packedOutput_t unpacked_output = init_packed_output(PACKED_OUT_ALLOC_SZ);
        if (!unpacked_output.bBuf.buf) {
            return SYS_MALLOC_ERR;
        }

        try {
            std::string pi_name{_pack_instruct_name};
            packItem_t root_item{};
            root_item.name = strdup(pi_name.c_str());
            const auto free_root = irods::at_scope_exit{[&root_item] { std::free(root_item.name); }};

            const int status = unpack_lite3_obj_to_struct(_buf, 0, unpacked_output, root_item, _pack_table, nullptr);
            if (status < 0) {
                std::free(unpacked_output.bBuf.buf);
                return status;
            }

            *_out_struct = unpacked_output.bBuf.buf;
            return 0;
        }
        catch (const lite3cpp::exception&) {
            std::free(unpacked_output.bBuf.buf);
            return SYS_PACK_INSTRUCT_FORMAT_ERR;
        }
        catch (const std::bad_alloc&) {
            std::free(unpacked_output.bBuf.buf);
            return SYS_MALLOC_ERR;
        }
        catch (...) {
            std::free(unpacked_output.bBuf.buf);
            return SYS_INTERNAL_ERR;
        }
    }

    auto decode_to_lite3(std::span<const std::byte> _wire_bytes,
                         std::string_view _pack_instruct_name,
                         irodsProt_t _wire_prot,
                         std::string_view _peer_version,
                         const packInstruct_t* _pack_table) -> expected<lite3cpp::Buffer, int>
    {
        if (_wire_bytes.empty() || _pack_instruct_name.empty()) {
            return unexpected{USER_PACKSTRUCT_INPUT_ERR};
        }

        switch (_wire_prot) {
        case LITE3_PROT: {
            if (_wire_bytes.size() < lite3cpp::config::node_size) {
                return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
            }
            return lite3cpp::Buffer(reinterpret_cast<const std::uint8_t*>(_wire_bytes.data()), _wire_bytes.size());
        }
        case JSON_PROT: {
            try {
                std::string json_str(reinterpret_cast<const char*>(_wire_bytes.data()), _wire_bytes.size());
                while (!json_str.empty() && json_str.back() == '\0') {
                    json_str.pop_back();
                }
                return lite3cpp::lite3_json::from_json_string(json_str);
            }
            catch (const lite3cpp::exception&) {
                return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
            }
            catch (...) {
                return unexpected{SYS_INTERNAL_ERR};
            }
        }
        case XML_PROT:
        case NATIVE_PROT: {
            std::string pi_name{_pack_instruct_name};
            std::string peer_ver{_peer_version};
            const char* peer_ver_ptr = peer_ver.empty() ? nullptr : peer_ver.c_str();

            const void* wire_ptr = _wire_bytes.data();
            std::vector<char> xml_null_term;
            if (_wire_prot == XML_PROT && _wire_bytes.back() != std::byte{0}) {
                xml_null_term.resize(_wire_bytes.size() + 1, '\0');
                std::memcpy(xml_null_term.data(), _wire_bytes.data(), _wire_bytes.size());
                wire_ptr = xml_null_term.data();
            }

            void* tmp_struct = nullptr;
            const int status =
                unpack_struct(wire_ptr, &tmp_struct, pi_name.c_str(), _pack_table, _wire_prot, peer_ver_ptr);
            if (status < 0 || !tmp_struct) {
                std::free(tmp_struct);
                return unexpected{status < 0 ? status : SYS_PACK_INSTRUCT_FORMAT_ERR};
            }

            const auto free_tmp = irods::at_scope_exit{[&tmp_struct] { std::free(tmp_struct); }};
            return struct_to_lite3(tmp_struct, _pack_instruct_name, _pack_table, FREE_POINTER);
        }
        default:
            return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
        }
    }

    auto encode_from_lite3(const lite3cpp::Buffer& _msg_buf,
                           std::string_view _pack_instruct_name,
                           irodsProt_t _wire_prot,
                           std::string_view _peer_version,
                           const packInstruct_t* _pack_table) -> expected<bytesBuf_t*, int>
    {
        if (_msg_buf.size() < lite3cpp::config::node_size || _pack_instruct_name.empty()) {
            return unexpected{USER_PACKSTRUCT_INPUT_ERR};
        }

        switch (_wire_prot) {
        case LITE3_PROT: {
            auto* bbuf = static_cast<bytesBuf_t*>(std::malloc(sizeof(bytesBuf_t)));
            if (!bbuf) {
                return unexpected{SYS_MALLOC_ERR};
            }
            bbuf->len = static_cast<int>(_msg_buf.size());
            bbuf->buf = std::malloc(_msg_buf.size() + 1);
            if (!bbuf->buf) {
                std::free(bbuf);
                return unexpected{SYS_MALLOC_ERR};
            }
            std::memcpy(bbuf->buf, _msg_buf.data(), _msg_buf.size());
            static_cast<char*>(bbuf->buf)[_msg_buf.size()] = '\0';
            return bbuf;
        }
        case JSON_PROT: {
            try {
                const std::string json_str = lite3cpp::lite3_json::to_json_string(_msg_buf, 0);
                auto* bbuf = static_cast<bytesBuf_t*>(std::malloc(sizeof(bytesBuf_t)));
                if (!bbuf) {
                    return unexpected{SYS_MALLOC_ERR};
                }
                bbuf->len = static_cast<int>(json_str.size());
                bbuf->buf = std::malloc(json_str.size() + 1);
                if (!bbuf->buf) {
                    std::free(bbuf);
                    return unexpected{SYS_MALLOC_ERR};
                }
                std::memcpy(bbuf->buf, json_str.data(), json_str.size() + 1);
                return bbuf;
            }
            catch (const lite3cpp::exception&) {
                return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
            }
            catch (...) {
                return unexpected{SYS_INTERNAL_ERR};
            }
        }
        case XML_PROT:
        case NATIVE_PROT: {
            void* tmp_struct = nullptr;
            const int unpack_ec = lite3_to_struct(_msg_buf, &tmp_struct, _pack_instruct_name, _pack_table);
            if (unpack_ec < 0 || !tmp_struct) {
                std::free(tmp_struct);
                return unexpected{unpack_ec < 0 ? unpack_ec : SYS_PACK_INSTRUCT_FORMAT_ERR};
            }
            const auto free_tmp = irods::at_scope_exit{[&tmp_struct] { std::free(tmp_struct); }};

            std::string pi_name{_pack_instruct_name};
            std::string peer_ver{_peer_version};
            const char* peer_ver_ptr = peer_ver.empty() ? nullptr : peer_ver.c_str();

            bytesBuf_t* packed_bbuf = nullptr;
            const int pack_ec = pack_struct(
                tmp_struct, &packed_bbuf, pi_name.c_str(), _pack_table, FREE_POINTER, _wire_prot, peer_ver_ptr);
            if (pack_ec < 0 || !packed_bbuf) {
                freeBBuf(packed_bbuf);
                return unexpected{pack_ec < 0 ? pack_ec : SYS_PACK_INSTRUCT_FORMAT_ERR};
            }
            return packed_bbuf;
        }
        default:
            return unexpected{SYS_PACK_INSTRUCT_FORMAT_ERR};
        }
    }
} // namespace irods::wire
