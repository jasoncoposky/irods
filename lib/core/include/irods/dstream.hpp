#ifndef IRODS_IO_DSTREAM_HPP
#define IRODS_IO_DSTREAM_HPP

#include "irods/filesystem/path.hpp"
#include "irods/transport/transport.hpp"

#include <streambuf>
#include <type_traits>
#include <vector>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <utility>
#include <cstring>

namespace irods::experimental::io
{
    // Details about what each virtual function in this template are required to
    // do can be found at the following link:
    //
    //      https://en.cppreference.com/w/cpp/io/basic_streambuf
    //
    template <typename CharT,
              typename Traits = std::char_traits<CharT>>
    class basic_data_object_buf final
        : public std::basic_streambuf<CharT, Traits>
    {
    public:
        // clang-format off
        using char_type   = CharT;
        using traits_type = Traits;
        using int_type    = typename traits_type::int_type;
        using pos_type    = typename traits_type::pos_type;
        using off_type    = typename traits_type::off_type;
        // clang-format on

        static_assert(std::is_same_v<char_type, char>, R"(character type must be "char")");

    private:
        using base_type = std::basic_streambuf<CharT, Traits>;

        // clang-format off
        inline static constexpr std::size_t buffer_size   = 4096;

        // Errors
        inline static constexpr auto external_write_error = -1;
        inline static const     auto seek_error           = pos_type{off_type{-1}};
        // clang-format on

    public:
        basic_data_object_buf()
            : base_type{}
            , buf_{}
            , transport_{}
        {
        }

        basic_data_object_buf(basic_data_object_buf&& _other) noexcept
            : basic_data_object_buf{}
        {
            swap(_other);
        }

        basic_data_object_buf& operator=(basic_data_object_buf&& _other) noexcept
        {
            if (this != &_other) {
                close();
                transport_ = nullptr;
                this->setg(nullptr, nullptr, nullptr);
                this->setp(nullptr, nullptr);
                swap(_other);
            }
            return *this;
        }

        ~basic_data_object_buf() override
        {
            close();
        }

        void swap(basic_data_object_buf& _other) noexcept
        {
            using std::swap;

            base_type::swap(_other);
            swap(transport_, _other.transport_);
            swap(buf_, _other.buf_);
        }

        friend void swap(basic_data_object_buf& _lhs, basic_data_object_buf& _rhs) noexcept
        {
            _lhs.swap(_rhs);
        }

        bool is_open() const noexcept
        {
            return transport_ && transport_->is_open();
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const filesystem::path& _path,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_path, _mode);
            });
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const filesystem::path& _path,
                                    const replica_number& _replica_number,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_path, _replica_number, _mode);
            });
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const filesystem::path& _path,
                                    const root_resource_name& _root_resource_name,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_path, _root_resource_name, _mode);
            });
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const filesystem::path& _path,
                                    const leaf_resource_name& _leaf_resource_name,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_path, _leaf_resource_name, _mode);
            });
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const replica_token& _replica_token,
                                    const filesystem::path& _path,
                                    const replica_number& _replica_number,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_replica_token, _path, _replica_number, _mode);
            });
        }

        basic_data_object_buf* open(transport<char_type>& _transport,
                                    const replica_token& _replica_token,
                                    const filesystem::path& _path,
                                    const leaf_resource_name& _leaf_resource_name,
                                    std::ios_base::openmode _mode)
        {
            return open_impl(_transport, _mode, [&] {
                return _transport.open(_replica_token, _path, _leaf_resource_name, _mode);
            });
        }

        basic_data_object_buf* close(const on_close_success* _on_close_success = nullptr)
        {
            if (!is_open()) {
                return nullptr;
            }

            auto* sb = this;

            if (this->sync()) {
                sb = nullptr;
            }

            if (!transport_->close(_on_close_success)) {
                sb = nullptr;
            }

            this->setg(nullptr, nullptr, nullptr);
            this->setp(nullptr, nullptr);

            return sb;
        }

        int file_descriptor() const noexcept
        {
            return transport_ ? transport_->file_descriptor() : -1;
        }

        const root_resource_name& root_resource_name() const
        {
            return transport_->root_resource_name();
        }

        const leaf_resource_name& leaf_resource_name() const
        {
            return transport_->leaf_resource_name();
        }

        const replica_number& replica_number() const
        {
            return transport_->replica_number();
        }

        const replica_token& replica_token() const
        {
            return transport_->replica_token();
        }

        [[nodiscard]] int last_error() const noexcept
        {
            return transport_ ? transport_->last_error() : 0;
        }

    protected:
        int_type underflow() override
        {
            if (!is_open()) {
                return traits_type::eof();
            }

            prepare_for_input();

            // If the "Get" area has not been fully consumed, then return the
            // current character.
            if (this->gptr() < this->egptr()) {
                return traits_type::to_int_type(*this->gptr());
            }

            // The "Get" area has been consumed. Fill the internal buffer with
            // new data from the data object.

            const auto bytes_read = transport_->receive(
                buf_.data(),
                static_cast<std::streamsize>(buf_.size() * sizeof(char_type)));

            if (bytes_read <= 0) {
                return traits_type::eof();
            }

            auto* pbase = buf_.data();
            this->setg(pbase, pbase, pbase + bytes_read);

            return traits_type::to_int_type(*this->gptr());
        }

        int_type overflow(int_type _c = traits_type::eof()) override
        {
            if (!is_open()) {
                return traits_type::eof();
            }

            prepare_for_output();

            if (flush_buffer() == external_write_error) {
                return traits_type::eof();
            }

            if (!traits_type::eq_int_type(_c, traits_type::eof())) {
                return this->sputc(traits_type::to_char_type(_c));
            }

            return traits_type::not_eof(_c);
        }

        std::streamsize xsgetn(char_type* _buffer, std::streamsize _buffer_size) override
        {
            if (!is_open() || _buffer_size <= 0) {
                return 0;
            }

            prepare_for_input();

            const auto avail = static_cast<std::streamsize>(this->egptr() - this->gptr());
            const auto bytes_to_copy = std::min(avail, _buffer_size);

            // If there are bytes in the internal buffer that haven't been consumed,
            // then copy up to "_buffer_size" bytes from the internal buffer into "_buffer".
            if (bytes_to_copy > 0) {
                std::memcpy(_buffer, this->gptr(), static_cast<std::size_t>(bytes_to_copy) * sizeof(char_type));
                this->gbump(static_cast<int>(bytes_to_copy));
            }

            const auto remaining = _buffer_size - bytes_to_copy;
            if (remaining == 0) {
                return bytes_to_copy;
            }

            // Zero-copy fast path when the remaining request is at least as large as the internal buffer.
            if (remaining >= static_cast<std::streamsize>(buf_.size())) {
                const auto bytes_read = transport_->receive(
                    _buffer + bytes_to_copy,
                    remaining * static_cast<std::streamsize>(sizeof(char_type)));
                if (bytes_read <= 0) {
                    return bytes_to_copy;
                }

                return bytes_to_copy + bytes_read;
            }

            // Buffered path for small reads to avoid issuing an RPC on every small read.
            const auto bytes_read = transport_->receive(
                buf_.data(),
                static_cast<std::streamsize>(buf_.size() * sizeof(char_type)));
            if (bytes_read <= 0) {
                return bytes_to_copy;
            }

            auto* pbase = buf_.data();
            const auto chunk = std::min(remaining, bytes_read);
            std::memcpy(_buffer + bytes_to_copy, pbase, static_cast<std::size_t>(chunk) * sizeof(char_type));
            this->setg(pbase, pbase + chunk, pbase + bytes_read);

            return bytes_to_copy + chunk;
        }

        std::streamsize xsputn(const char_type* _buffer, std::streamsize _buffer_size) override
        {
            if (!is_open() || _buffer_size <= 0) {
                return 0;
            }

            prepare_for_output();

            if (flush_buffer() == external_write_error) {
                return 0;
            }

            const auto bytes_written = transport_->send(
                _buffer,
                _buffer_size * static_cast<std::streamsize>(sizeof(char_type)));
            if (bytes_written <= 0) {
                return 0;
            }

            return bytes_written;
        }

        int sync() override
        {
            if (this->pptr()) {
                return flush_buffer();
            }

            return 0;
        }

        int_type pbackfail(int_type _c = traits_type::eof()) override
        {
            // If the "next" pointer of the "Get" area points to the beginning of the
            // "Get" area, then return immediately.
            if (!this->gptr() || this->gptr() == this->eback()) {
                return traits_type::eof();
            }

            // Decrement the "next" pointer.
            this->gbump(-1);

            // If the "next" pointer is not pointing to "EOF", then store "_c" at the location
            // pointed at by the "next" pointer.
            if (!traits_type::eq_int_type(_c, traits_type::eof())) {
                *this->gptr() = traits_type::to_char_type(_c);
            }

            return traits_type::not_eof(_c);
        }

        pos_type seekoff(off_type _off,
                         std::ios_base::seekdir _dir,
                         std::ios_base::openmode _which = std::ios_base::in | std::ios_base::out) override
        {
            (void) _which;

            if (!is_open()) {
                return seek_error;
            }

            if (this->sync() != 0) {
                return seek_error;
            }

            if (this->gptr() < this->egptr() && _dir == std::ios_base::cur) {
                _off -= static_cast<off_type>(this->egptr() - this->gptr());
            }

            const auto pos = transport_->seekpos(_off, _dir);
            if (pos != seek_error && this->gptr()) {
                auto* pbase = buf_.data();
                this->setg(pbase, pbase, pbase);
            }

            return pos;
        }

        pos_type seekpos(pos_type _pos, std::ios_base::openmode _which = std::ios_base::in | std::ios_base::out) override
        {
            return seekoff(off_type{_pos}, std::ios_base::beg, _which);
        }

    private:
        template <typename OpenFunc>
        basic_data_object_buf* open_impl(transport<char_type>& _transport,
                                         std::ios_base::openmode _mode,
                                         OpenFunc&& _open_func)
        {
            if (is_open()) {
                return nullptr;
            }

            transport_ = &_transport;

            if (!_open_func()) {
                return nullptr;
            }

            if (buf_.empty()) {
                buf_.resize(buffer_size);
            }

            this->setg(nullptr, nullptr, nullptr);
            this->setp(nullptr, nullptr);
            init_get_or_put_area(_mode);

            return this;
        }

        void prepare_for_input()
        {
            // Return if the pointers of "Get" area have already been set up.
            if (this->gptr()) {
                return;
            }

            // Flush and clear the contents of the "Put" area.
            this->sync();
            this->setp(nullptr, nullptr);

            // Setup the "Get" area.
            auto* pbase = buf_.data();
            this->setg(pbase, pbase, pbase);
        }

        void prepare_for_output()
        {
            // Return if the pointers of "Put" area have already been set up.
            if (this->pptr()) {
                return;
            }

            // Clear the contents of the "Get" area.
            this->setg(nullptr, nullptr, nullptr);

            // Setup the "Put" area.
            auto* pbase = buf_.data();
            this->setp(pbase, pbase + buf_.size());
        }

        void init_get_or_put_area(std::ios_base::openmode _mode) noexcept
        {
            using std::ios_base;

            const auto m = ios_base::in | ios_base::out;

            if ((_mode & m) == m || _mode & ios_base::in) {
                prepare_for_input();
            }
            else if (_mode & ios_base::out) {
                prepare_for_output();
            }
        }

        int flush_buffer()
        {
            const auto bytes_to_send = this->pptr() - this->pbase();

            if (bytes_to_send <= 0) {
                return 0;
            }

            const auto bytes_written = transport_->send(
                this->pbase(),
                bytes_to_send * static_cast<std::streamsize>(sizeof(char_type)));

            if (bytes_written <= 0) {
                return external_write_error;
            }

            if (bytes_written < bytes_to_send) {
                const auto remaining = bytes_to_send - bytes_written;
                std::memmove(
                    buf_.data(),
                    this->pbase() + bytes_written,
                    static_cast<std::size_t>(remaining) * sizeof(char_type));
                this->pbump(-static_cast<int>(bytes_written));
                return external_write_error;
            }

            this->pbump(-static_cast<int>(bytes_written));

            return 0;
        }

        std::vector<char_type> buf_;
        transport<char_type>* transport_;
    }; // basic_data_object_buf

    // Provides a default openmode for basic_dstream constructors and open()
    // member functions based on the type "T". "T" must be one of the general
    // stream types defined by the C++ standard (i.e. basic_istream, basic_ostream,
    // or basic_iostream).
    template <typename>
    inline static constexpr std::ios_base::openmode default_openmode{};

    template <typename T>
    inline static constexpr std::ios_base::openmode default_openmode<std::basic_istream<T>> = std::ios_base::in;

    template <typename T>
    inline static constexpr std::ios_base::openmode default_openmode<std::basic_ostream<T>> = std::ios_base::out;

    template <typename T>
    inline static constexpr std::ios_base::openmode default_openmode<std::basic_iostream<T>> = std::ios_base::in | std::ios_base::out;

    // mandatory_openmode used to be private members of basic_dstream, but due
    // to a bug in GCC (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=90031),
    // it was moved outside of the class.
    namespace detail {
        template <typename>
        inline static constexpr std::ios_base::openmode mandatory_openmode{};

        template <typename T>
        inline static constexpr std::ios_base::openmode mandatory_openmode<std::basic_istream<T>> = std::ios_base::in;

        template <typename T>
        inline static constexpr std::ios_base::openmode mandatory_openmode<std::basic_ostream<T>> = std::ios_base::out;
    }

    // A concrete stream class template that wraps a basic_data_object_buf object.
    // The general stream used to instantiate this type must use "char" for the underlying
    // character type. Using wchar_t or anything else to instantiate this template is undefined.
    // This class is modeled after the C++ standard file stream classes.
    template <typename GeneralStream>
    class basic_dstream final
        : public GeneralStream
    {
    public:
        // clang-format off
        using char_type   = typename GeneralStream::char_type;
        using traits_type = typename GeneralStream::traits_type;
        using int_type    = typename traits_type::int_type;
        using pos_type    = typename traits_type::pos_type;
        using off_type    = typename traits_type::off_type;
        // clang-format on

        static_assert(std::is_same_v<char_type, char>, R"(character type must be "char")");

        basic_dstream()
            : GeneralStream{&buf_}
            , buf_{}
        {
        }

        basic_dstream(transport<char_type>& _transport,
                      const filesystem::path& _path,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _path, _mode);
        }

        basic_dstream(transport<char_type>& _transport,
                      const filesystem::path& _path,
                      const replica_number& _replica_number,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _path, _replica_number, _mode);
        }

        basic_dstream(transport<char_type>& _transport,
                      const filesystem::path& _path,
                      const root_resource_name& _root_resource_name,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _path, _root_resource_name, _mode);
        }

        basic_dstream(transport<char_type>& _transport,
                      const filesystem::path& _path,
                      const leaf_resource_name& _leaf_resource_name,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _path, _leaf_resource_name, _mode);
        }

        basic_dstream(transport<char_type>& _transport,
                      const replica_token& _replica_token,
                      const filesystem::path& _path,
                      const replica_number& _replica_number,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _replica_token, _path, _replica_number, _mode);
        }

        basic_dstream(transport<char_type>& _transport,
                      const replica_token& _replica_token,
                      const filesystem::path& _path,
                      const leaf_resource_name& _leaf_resource_name,
                      std::ios_base::openmode _mode = default_openmode<GeneralStream>)
            : basic_dstream{}
        {
            open(_transport, _replica_token, _path, _leaf_resource_name, _mode);
        }

        basic_dstream(basic_dstream&& _other)
            : GeneralStream{std::move(_other)}
            , buf_{std::move(_other.buf_)}
        {
            this->set_rdbuf(&buf_);
        }

        basic_dstream& operator=(basic_dstream&& _other)
        {
            GeneralStream::operator=(std::move(_other));
            buf_ = std::move(_other.buf_);
            return *this;
        }

        ~basic_dstream() = default;

        void swap(basic_dstream& _other)
        {
            GeneralStream::swap(_other);
            buf_.swap(_other.buf_);
        }

        friend void swap(basic_dstream& _lhs, basic_dstream& _rhs)
        {
            _lhs.swap(_rhs);
        }

        basic_data_object_buf<char_type, traits_type>* rdbuf() const
        {
            return const_cast<basic_data_object_buf<char_type, traits_type>*>(&buf_);
        }

        bool is_open() const noexcept
        {
            return buf_.is_open();
        }

        void open(transport<char_type>& _transport,
                  const filesystem::path& _path,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _path, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void open(transport<char_type>& _transport,
                  const filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _path, _replica_number, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void open(transport<char_type>& _transport,
                  const filesystem::path& _path,
                  const root_resource_name& _root_resource_name,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _path, _root_resource_name, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void open(transport<char_type>& _transport,
                  const filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _path, _leaf_resource_name, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void open(transport<char_type>& _transport,
                  const replica_token& _replica_token,
                  const filesystem::path& _path,
                  const replica_number& _replica_number,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _replica_token, _path, _replica_number, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void open(transport<char_type>& _transport,
                  const replica_token& _replica_token,
                  const filesystem::path& _path,
                  const leaf_resource_name& _leaf_resource_name,
                  std::ios_base::openmode _mode = default_openmode<GeneralStream>)
        {
            if (!buf_.open(_transport, _replica_token, _path, _leaf_resource_name, _mode | detail::mandatory_openmode<GeneralStream>)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        void close(const on_close_success* _on_close_success = nullptr)
        {
            if (!buf_.close(_on_close_success)) {
                this->setstate(std::ios_base::failbit);
            }
            else {
                this->clear();
            }
        }

        int file_descriptor() const noexcept
        {
            return buf_.file_descriptor();
        }

        const root_resource_name& root_resource_name() const
        {
            return buf_.root_resource_name();
        }

        const leaf_resource_name& leaf_resource_name() const
        {
            return buf_.leaf_resource_name();
        }

        const replica_number& replica_number() const
        {
            return buf_.replica_number();
        }

        const replica_token& replica_token() const
        {
            return buf_.replica_token();
        }

        [[nodiscard]] int last_error() const noexcept
        {
            return buf_.last_error();
        }

    private:
        basic_data_object_buf<char_type, traits_type> buf_;
    }; // basic_dstream

    // clang-format off
    //
    // The concrete stream types.
    // For details on how to use these types, refer to the following links:
    //
    //    https://en.cppreference.com/w/cpp/io/basic_fstream
    //    https://en.cppreference.com/w/cpp/io/basic_ifstream
    //    https://en.cppreference.com/w/cpp/io/basic_ofstream
    //
    using data_object_buf = basic_data_object_buf<char>;
    using idstream        = basic_dstream<std::basic_istream<char>>;
    using odstream        = basic_dstream<std::basic_ostream<char>>;
    using dstream         = basic_dstream<std::basic_iostream<char>>;
    // clang-format on
} // namespace irods::experimental::io

#endif // IRODS_IO_DSTREAM_HPP

