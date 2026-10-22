#ifndef IRODS_QUERY_PROCESSOR_HPP
#define IRODS_QUERY_PROCESSOR_HPP

#include "irods/thread_pool.hpp"
#include "irods/genquery2_builder.hpp"
#include "irods/irods_exception.hpp"
#include "irods/rodsErrorTable.h"

#include <string>
#include <functional>
#include <future>
#include <vector>
#include <tuple>
#include <exception>
#include <utility>

namespace irods
{
    template <typename ConnectionType>
    class query_processor
    {
    public:
        // clang-format off
        using error        = std::tuple<int, std::string>;
        using errors       = std::vector<error>;
        using result_row   = std::vector<std::string>;
        using job          = std::function<void (const result_row&)>;
        // clang-format on

        class future
        {
        public:
            auto get() -> errors
            {
                errors errs;
                errs.reserve(promises.size());

                for (auto&& p : promises) {
                    auto&& e = p->get_future().get();
                    if (std::get<0>(e) < 0) {
                        errs.push_back(std::move(e));
                    }
                }

                return errs;
            }

            auto size() const noexcept -> std::uint32_t
            {
                return promises.size();
            }

            friend query_processor;

        private:
            auto push_back(std::shared_ptr<std::promise<error>> p) -> void
            {
                promises.push_back(p);
            }

            std::vector<std::shared_ptr<std::promise<error>>> promises;
        }; // class future

        query_processor(irods::experimental::genquery2::select _select,
                        job _job,
                        uint32_t _limit = 0)
            : select_{std::move(_select)}
            , job_{std::move(_job)}
            , limit_{_limit}
        {
        }

        query_processor(const irods::experimental::genquery2::builder::select_builder& _builder,
                        job _job,
                        uint32_t _limit = 0)
            : select_{_builder.build()}
            , job_{std::move(_job)}
            , limit_{_limit}
        {
        }

        query_processor(const query_processor&) = delete;
        query_processor& operator=(const query_processor&) = delete;

        auto execute(thread_pool& _thread_pool, ConnectionType& _conn) -> future
        {
            future f;
            const auto rows = irods::experimental::genquery2::execute(_conn, select_);
            std::uint32_t count = 0;

            for (const auto& json_row : rows) {
                if (limit_ > 0 && count >= limit_) {
                    break;
                }
                ++count;

                auto r = json_row.template get<result_row>();
                auto p = std::make_shared<std::promise<error>>();
                f.push_back(p);

                thread_pool::post(_thread_pool, [this, p, r = std::move(r)]() mutable noexcept {
                    try {
                        job_(r);
                        p->set_value({0, ""});
                    }
                    catch (const irods::exception& e) {
                        p->set_value({e.code(), e.what()});
                    }
                    catch (const std::exception& e) {
                        p->set_value({SYS_UNKNOWN_ERROR, e.what()});
                    }
                    catch (...) {
                        p->set_value({SYS_UNKNOWN_ERROR, "Unknown error occurred while processing job."});
                    }
                });

            } // for row

            return f;
        }

    private:
        irods::experimental::genquery2::select select_;
        job job_;
        uint32_t limit_;
    }; // class query_processor
} // namespace irods

#endif // IRODS_QUERY_PROCESSOR_HPP

