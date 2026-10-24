#ifndef IRODS_DELAY_QUEUE_HPP
#define IRODS_DELAY_QUEUE_HPP

#include "irods/fixed_buffer_resource.hpp"

#include <cstddef>
#include <algorithm>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <string>
#include <vector>

namespace irods
{
    class delay_queue // NOLINT(cppcoreguidelines-special-member-functions)
    {
      public:
        explicit delay_queue(std::int64_t _pool_size_in_bytes)
        {
            namespace ipmr = experimental::pmr;

            if (_pool_size_in_bytes > 0) {
                buffer_.resize(_pool_size_in_bytes);

                // DO NOT wrap this memory resource in a pool resource (e.g. unsynchronized_pool_resource). Doing
                // so has shown that it can lead to the delay server not making progress on delay rule execution.
                // The goal of this memory resource is to limit memory usage. Wrapping it adds no additional value.
                allocator_ = std::make_unique<ipmr::fixed_buffer_resource<std::byte>>(buffer_.data(), buffer_.size());
                queued_rules_ = std::make_unique<std::pmr::vector<std::pmr::string>>(allocator_.get());
            }
            else {
                queued_rules_ = std::make_unique<std::pmr::vector<std::pmr::string>>(std::pmr::new_delete_resource());
            }
        }

        delay_queue(const delay_queue&) = delete;
        delay_queue& operator=(const delay_queue&) = delete;

        bool contains_rule_id(const std::string& _rule_id)
        {
            std::lock_guard rules_lock{rules_mutex_};
            return std::any_of(queued_rules_->begin(), queued_rules_->end(), [&](const auto& q_rule) {
                return q_rule.data() == _rule_id;
            });
        }

        void enqueue_rule(const std::string& rule_id)
        {
            std::lock_guard rules_lock{rules_mutex_};
            queued_rules_->emplace_back(rule_id.data());
        }

        void dequeue_rule(const std::string& rule_id)
        {
            std::lock_guard rules_lock{rules_mutex_};
            for (auto it = queued_rules_->begin(); it != queued_rules_->end(); ++it) {
                if (*it == rule_id.data()) {
                    queued_rules_->erase(it);
                    break;
                }
            }
        }

      private:
        std::mutex rules_mutex_;
        std::vector<std::byte> buffer_;
        std::unique_ptr<std::pmr::memory_resource> allocator_;
        std::unique_ptr<std::pmr::vector<std::pmr::string>> queued_rules_;
    }; // delay_queue
} // namespace irods

#endif // IRODS_DELAY_QUEUE_HPP
