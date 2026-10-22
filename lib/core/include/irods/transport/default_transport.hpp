#include "irods/rodsErrorTable.h"
#undef IRODS_IO_DEFAULT_TRANSPORT_HPP_INCLUDE_HEADER

#if defined(IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API)
#  if !defined(IRODS_IO_DEFAULT_TRANSPORT_HPP_FOR_SERVER)
#    define IRODS_IO_DEFAULT_TRANSPORT_HPP_FOR_SERVER
#    define IRODS_IO_DEFAULT_TRANSPORT_HPP_INCLUDE_HEADER
#  endif
#elif !defined(IRODS_IO_DEFAULT_TRANSPORT_HPP_FOR_CLIENT)
#  define IRODS_IO_DEFAULT_TRANSPORT_HPP_FOR_CLIENT
#  define IRODS_IO_DEFAULT_TRANSPORT_HPP_INCLUDE_HEADER
#endif

#ifdef IRODS_IO_DEFAULT_TRANSPORT_HPP_INCLUDE_HEADER

#undef NAMESPACE_IMPL
#undef rxComm
#undef rxDataObjRead
#undef rxDataObjWrite
#undef rxDataObjLseek
#undef rx_replica_close
#undef rx_replica_open

// clang-format off
#ifdef IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
#  include "irods/rs_replica_open.hpp"
#  include "irods/rs_replica_close.hpp"
#  include "irods/rsDataObjRead.hpp"
#  include "irods/rsDataObjWrite.hpp"
#  include "irods/rsDataObjLseek.hpp"

#  define NAMESPACE_IMPL                  server

#  define rxComm                          rsComm_t

#  define rxDataObjRead                   rsDataObjRead
#  define rxDataObjWrite                  rsDataObjWrite
#  define rxDataObjLseek                  rsDataObjLseek
#  define rx_replica_open                 rs_replica_open
#  define rx_replica_close                rs_replica_close
#else
#  include "irods/replica_open.h"
#  include "irods/replica_close.h"
#  include "irods/dataObjRead.h"
#  include "irods/dataObjWrite.h"
#  include "irods/dataObjLseek.h"

#  define NAMESPACE_IMPL                  client

#  define rxComm                          rcComm_t

#  define rxDataObjRead                   rcDataObjRead
#  define rxDataObjWrite                  rcDataObjWrite
#  define rxDataObjLseek                  rcDataObjLseek
#  define rx_replica_open                 rc_replica_open
#  define rx_replica_close                rc_replica_close
#endif // IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
// clang-format on

#include "irods/rcMisc.h"
#include "irods/transport/transport.hpp"
#include "irods/irods_at_scope_exit.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <array>
#include <charconv>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>

namespace irods::experimental::io::NAMESPACE_IMPL
{
    template <typename CharT>
    class basic_transport : public transport<CharT>
    {
    public:
        // clang-format off
        using char_type   = typename transport<CharT>::char_type;
        using traits_type = typename transport<CharT>::traits_type;
        using int_type    = typename traits_type::int_type;
        using pos_type    = typename traits_type::pos_type;
        using off_type    = typename traits_type::off_type;
        // clang-format on

    private:
        // clang-format off
        inline static constexpr auto uninitialized_file_descriptor = -1;
        inline static constexpr auto minimum_valid_file_descriptor = 3;

        // Errors
        inline static constexpr auto translation_error             = -1;
        inline static const     auto seek_error                    = pos_type{off_type{-1}};
        // clang-format on

        struct cond_input_binder
        {
#ifdef IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
            keyValPair_t& cond;

            explicit cond_input_binder(keyValPair_t& _cond) noexcept
                : cond{_cond}
            {
            }

            ~cond_input_binder()
            {
                clearKeyVal(&cond);
            }

            void add(const char* _key, const std::string& _val) noexcept
            {
                addKeyVal(&cond, _key, _val.c_str());
            }

            void add_int(const char* _key, int _val) noexcept
            {
                std::array<char, 16> buf{};
                auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size() - 1, _val);
                *ptr = '\0';
                addKeyVal(&cond, _key, buf.data());
            }
#else
            keyValPair_t& cond;
            std::array<char*, 2> keys{};
            std::array<char*, 2> vals{};
            std::array<char, 16> repl_num_buf{};
            int len = 0;

            explicit cond_input_binder(keyValPair_t& _cond) noexcept
                : cond{_cond}
            {
            }

            ~cond_input_binder()
            {
                cond.len = 0;
                cond.keyWord = nullptr;
                cond.value = nullptr;
            }

            void add(const char* _key, const std::string& _val) noexcept
            {
                keys[len] = const_cast<char*>(_key);
                vals[len] = const_cast<char*>(_val.c_str());
                ++len;
                cond.len = len;
                cond.keyWord = keys.data();
                cond.value = vals.data();
            }

            void add_int(const char* _key, int _val) noexcept
            {
                auto [ptr, ec] = std::to_chars(repl_num_buf.data(), repl_num_buf.data() + repl_num_buf.size() - 1, _val);
                *ptr = '\0';
                keys[len] = const_cast<char*>(_key);
                vals[len] = repl_num_buf.data();
                ++len;
                cond.len = len;
                cond.keyWord = keys.data();
                cond.value = vals.data();
            }
#endif
        };

    public:
        explicit basic_transport(rxComm& _comm) noexcept
            : transport<CharT>{}
            , comm_{&_comm}
            , fd_{uninitialized_file_descriptor}
            , root_resc_name_{}
            , leaf_resc_name_{}
            , replica_number_{}
            , replica_token_{}
        {
        }

        basic_transport(const basic_transport&) = delete;
        basic_transport& operator=(const basic_transport&) = delete;

        basic_transport(basic_transport&& _other) noexcept
            : transport<CharT>{}
            , comm_{std::exchange(_other.comm_, nullptr)}
            , fd_{std::exchange(_other.fd_, uninitialized_file_descriptor)}
            , root_resc_name_{std::move(_other.root_resc_name_)}
            , leaf_resc_name_{std::move(_other.leaf_resc_name_)}
            , replica_number_{std::exchange(_other.replica_number_, {})}
            , replica_token_{std::move(_other.replica_token_)}
            , last_error_{std::exchange(_other.last_error_, 0)}
        {
        }

        basic_transport& operator=(basic_transport&& _other) noexcept
        {
            if (this != &_other) {
                comm_ = std::exchange(_other.comm_, nullptr);
                fd_ = std::exchange(_other.fd_, uninitialized_file_descriptor);
                root_resc_name_ = std::move(_other.root_resc_name_);
                leaf_resc_name_ = std::move(_other.leaf_resc_name_);
                replica_number_ = std::exchange(_other.replica_number_, {});
                replica_token_ = std::move(_other.replica_token_);
                last_error_ = std::exchange(_other.last_error_, 0);
            }
            return *this;
        }

        ~basic_transport() override = default;

        bool open(const irods::filesystem::path& _path,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [](auto&, auto&) {});
        }

        bool open(const irods::filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [_replica_number](auto& _input, auto& _cond) {
                _cond.add_int(REPL_NUM_KW, _replica_number.value);

                // Providing a replica number implies that the replica already exists.
                // This constructor does not support creation of new replicas.
                _input.openFlags &= ~O_CREAT;
            });
        }

        bool open(const irods::filesystem::path& _path,
                  const root_resource_name& _root_resource_name,
                  std::ios_base::openmode _mode) override
        {
            // This leaves the decision making to the server (i.e. the policy).
            return open_impl(_path, _mode, [&_root_resource_name](auto&, auto& _cond) {
                _cond.add(RESC_NAME_KW, _root_resource_name.value);
            });
        }

        bool open(const irods::filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode) override
        {
            // This is when the client knows exactly where the replica should reside.
            return open_impl(_path, _mode, [&_leaf_resource_name](auto&, auto& _cond) {
                _cond.add(LEAF_RESOURCE_NAME_KW, _leaf_resource_name.value);
            });
        }

        bool open(const replica_token& _replica_token,
                  const irods::filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [&_replica_token, _replica_number](auto& _input, auto& _cond) {
                _cond.add(REPLICA_TOKEN_KW, _replica_token.value);
                _cond.add_int(REPL_NUM_KW, _replica_number.value);

                // Providing a replica number implies that the replica already exists.
                // This constructor does not support creation of new replicas.
                _input.openFlags &= ~O_CREAT;
            });
        }

        bool open(const replica_token& _replica_token,
                  const irods::filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode) override
        {
            return open_impl(_path, _mode, [&_replica_token, &_leaf_resource_name](auto&, auto& _cond) {
                _cond.add(REPLICA_TOKEN_KW, _replica_token.value);
                _cond.add(LEAF_RESOURCE_NAME_KW, _leaf_resource_name.value);
            });
        }

        bool close(const on_close_success* _on_close_success = nullptr) override
        {
            if (!is_open()) {
                last_error_ = FILE_INDEX_LOOKUP_ERR;
                return false;
            }

            std::array<char, 256> json_buf{};
            char* end_ptr = nullptr;

            if (_on_close_success) {
                end_ptr = fmt::format_to_n(
                    json_buf.data(),
                    json_buf.size() - 1,
                    R"({{"fd":{},"update_size":{},"update_status":{},"compute_checksum":{},"send_notifications":{},"preserve_replica_state_table":{}}})",
                    fd_,
                    _on_close_success->update_size,
                    _on_close_success->update_status,
                    _on_close_success->compute_checksum,
                    _on_close_success->send_notifications,
                    _on_close_success->preserve_replica_state_table).out;
            }
            else {
                end_ptr = fmt::format_to_n(json_buf.data(), json_buf.size() - 1, R"({{"fd":{}}})", fd_).out;
            }
            *end_ptr = '\0';

            last_error_ = rx_replica_close(comm_, json_buf.data());
            if (0 != last_error_) {
                return false;
            }

            fd_ = uninitialized_file_descriptor;

            return true;
        }

        std::streamsize receive(char_type* _buffer, std::streamsize _buffer_size) override
        {
            if (!std::in_range<int>(_buffer_size)) {
                return (last_error_ = NARROWING_CONVERSION_ERROR);
            }

            openedDataObjInp_t input{};
            input.l1descInx = fd_;
            input.len = static_cast<int>(_buffer_size);

            bytesBuf_t output{};
            output.len = input.len;
            output.buf = _buffer;

            last_error_ = rxDataObjRead(comm_, &input, &output);

            return last_error_;
        }

        std::streamsize send(const char_type* _buffer, std::streamsize _buffer_size) override
        {
            if (!std::in_range<int>(_buffer_size)) {
                return (last_error_ = NARROWING_CONVERSION_ERROR);
            }

            openedDataObjInp_t input{};
            input.l1descInx = fd_;
            input.len = static_cast<int>(_buffer_size);

            bytesBuf_t input_buffer{};
            input_buffer.len = input.len;
            input_buffer.buf = const_cast<char_type*>(_buffer);

            last_error_ = rxDataObjWrite(comm_, &input, &input_buffer);

            return last_error_;
        }

        pos_type seekpos(off_type _offset, std::ios_base::seekdir _dir) override
        {
            openedDataObjInp_t input{};
            input.l1descInx = fd_;
            input.offset = _offset;

            switch (_dir) {
                case std::ios_base::beg:
                    input.whence = SEEK_SET;
                    break;

                case std::ios_base::cur:
                    input.whence = SEEK_CUR;
                    break;

                case std::ios_base::end:
                    input.whence = SEEK_END;
                    break;

                default:
                    last_error_ = SYS_INVALID_INPUT_PARAM;
                    return seek_error;
            }

            fileLseekOut_t* output{};

            at_scope_exit free_memory{[&output] {
                if (output) { // NOLINT(readability-implicit-bool-conversion)
                    std::free(output); // NOLINTNEXTLINE(cppcoreguidelines-owning-memory, cppcoreguidelines-no-malloc)
                }
            }};

            last_error_ = rxDataObjLseek(comm_, &input, &output);
            if (last_error_ < 0 || !output) {
                return seek_error;
            }

            return output->offset;
        }

        bool is_open() const noexcept override
        {
            return fd_ >= minimum_valid_file_descriptor;
        }

        int file_descriptor() const noexcept override
        {
            return fd_;
        }

        const root_resource_name& root_resource_name() const override
        {
            return root_resc_name_;
        }

        const leaf_resource_name& leaf_resource_name() const override
        {
            return leaf_resc_name_;
        }

        const replica_number& replica_number() const override
        {
            return replica_number_;
        }

        const replica_token& replica_token() const override
        {
            return replica_token_;
        }

        [[nodiscard]] int last_error() const noexcept override
        {
            return last_error_;
        }

    private:
        int make_open_flags(std::ios_base::openmode _mode) noexcept
        {
            using std::ios_base;

            const auto m = _mode & ~(ios_base::ate | ios_base::binary);

            if (ios_base::in == m) {
                return O_RDONLY;
            }
            else if (ios_base::out == m || (ios_base::out | ios_base::trunc) == m) {
                return O_CREAT | O_WRONLY | O_TRUNC;
            }
            else if (ios_base::app == m || (ios_base::out | ios_base::app) == m) {
                return O_CREAT | O_WRONLY | O_APPEND;
            }
            else if ((ios_base::out | ios_base::in) == m) {
                return O_RDWR;
            }
            else if ((ios_base::out | ios_base::in | ios_base::trunc) == m) {
                return O_CREAT | O_RDWR | O_TRUNC;
            }
            else if ((ios_base::out | ios_base::in | ios_base::app) == m ||
                     (ios_base::in | ios_base::app) == m)
            {
                return O_CREAT | O_RDWR | O_APPEND;
            }

            return translation_error;
        }

        bool seek_to_end_if_required(std::ios_base::openmode _mode)
        {
            if (std::ios_base::ate & _mode) {
                if (seek_error == seekpos(0, std::ios_base::end)) {
                    return false;
                }
            }

            return true;
        }

        template <typename Function>
        bool open_impl(const filesystem::path& _path, std::ios_base::openmode _mode, Function _func)
        {
            if (is_open()) {
                last_error_ = SYS_NOT_ALLOWED;
                return false;
            }

            const auto& path_str = _path.string();
            if (path_str.size() >= sizeof(std::declval<dataObjInp_t>().objPath)) {
                last_error_ = USER_STRLEN_TOOLONG;
                return false;
            }

            const auto flags = make_open_flags(_mode);

            if (flags == translation_error) {
                last_error_ = SYS_INVALID_INPUT_PARAM;
                return false;
            }

            dataObjInp_t input{};
            cond_input_binder cond_binder{input.condInput};

            // Set dataSize to invalid size to signal unknown size for streaming.
            input.dataSize = -1;
            input.createMode = 0600;
            input.openFlags = flags;
            rstrcpy(input.objPath, path_str.c_str(), sizeof(input.objPath));

            _func(input, cond_binder);

            char* json_output{};
            at_scope_exit free_json_output{[&json_output] {
                if (json_output) {
                    std::free(json_output);
                }
            }};

            last_error_ = rx_replica_open(comm_, &input, &json_output);

            if (last_error_ < minimum_valid_file_descriptor) {
                return false;
            }

            fd_ = last_error_;

            auto close_preserving_error = [this](int _err) noexcept {
                close();
                last_error_ = _err;
                return false;
            };

            if (!json_output) {
                return close_preserving_error(SYS_INTERNAL_ERR);
            }

            const auto fd_info = nlohmann::json::parse(json_output, nullptr, false);
            if (fd_info.is_discarded() || !fd_info.is_object()) {
                return close_preserving_error(SYS_INTERNAL_ERR);
            }

            const auto dobj_it = fd_info.find("data_object_info");
            const auto token_it = fd_info.find("replica_token");
            if (dobj_it == fd_info.end() || !dobj_it->is_object() ||
                token_it == fd_info.end() || !token_it->is_string())
            {
                return close_preserving_error(SYS_INTERNAL_ERR);
            }

            const auto hier_it = dobj_it->find("resource_hierarchy");
            const auto repl_num_it = dobj_it->find("replica_number");
            if (hier_it == dobj_it->end() || !hier_it->is_string() ||
                repl_num_it == dobj_it->end() || !repl_num_it->is_number_integer())
            {
                return close_preserving_error(SYS_INTERNAL_ERR);
            }

            const std::string_view hier = hier_it->get_ref<const std::string&>();
            if (hier.empty()) {
                return close_preserving_error(SYS_INTERNAL_ERR);
            }

            const auto first_sep = hier.find(';');
            root_resc_name_.value.assign(hier.substr(0, first_sep));

            const auto last_sep = hier.rfind(';');
            leaf_resc_name_.value.assign(last_sep == std::string_view::npos ? hier : hier.substr(last_sep + 1));

            replica_number_.value = repl_num_it->get<int>();
            replica_token_.value = token_it->get_ref<const std::string&>();

            if (!seek_to_end_if_required(_mode)) {
                return close_preserving_error(last_error_);
            }

            return true;
        }

        rxComm* comm_;
        int fd_;
        struct root_resource_name root_resc_name_;
        struct leaf_resource_name leaf_resc_name_;
        struct replica_number replica_number_;
        struct replica_token replica_token_;
        int last_error_ = 0;
    }; // basic_transport

    // clang-format off
    using default_transport = basic_transport<char>;
    using native_transport  = basic_transport<char>;
    // clang-format on
} // irods::experimental::io::NAMESPACE_IMPL

#endif // IRODS_IO_DEFAULT_TRANSPORT_HPP_INCLUDE_HEADER
