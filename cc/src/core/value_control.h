// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <atomic>

namespace FASTER { namespace core {

    //------------------------------------------------------------------------------------------------------
    /// class value_control
    /// Union of payload type and control value
    //------------------------------------------------------------------------------------------------------
    template <class value_t, typename control_t, control_t default_value>
    class value_control
    {
    private:
      union controlled_value
      {
        value_t   layout_;
        control_t control_;

        controlled_value() noexcept
          : control_{default_value}
        {}
        explicit controlled_value(control_t control) noexcept
          : control_{control}
        {}
        explicit controlled_value(value_t layout) noexcept
          : layout_{layout}
        {}
      };

    private:
      controlled_value value_;

    public:
      value_control() = default;

      value_control(const value_control& other) noexcept
        : value_{other.control()}
      {}
      explicit value_control(value_t layout) noexcept
          : value_{layout}
      {}
      explicit value_control(control_t control) noexcept
          : value_{control}
      {}
      value_control& operator=(const value_control& other) noexcept
      {
        value_.control_ = other.control();
        return *this;
      }
      bool operator ==(const value_control& other) const noexcept
      {
        return control() == other.control();
      }
      bool operator !=(const value_control& other) const noexcept
      {
        return control() != other.control();
      }
      bool operator ==(control_t ctrl) const noexcept
      {
        return control() == ctrl;
      }
      bool operator !=(control_t ctrl) const noexcept
      {
        return control() != ctrl;
      }
      control_t control() const noexcept
      {
        static_assert(sizeof(control_t) <= 64, "Consider to make control getter by reference");
        return value_.control_;
      }
      control_t& control_ref() noexcept
      {
          return value_.control_;
      }
      const value_t& value() const noexcept
      {
          return value_.layout_;
      }
      value_t& value() noexcept
      {
          return value_.layout_;
      }
    };
    //------------------------------------------------------------------------------------------------------
    /// struct value_control
    /// Union of payload type and atomic control value
    //------------------------------------------------------------------------------------------------------
    template <class value_control_t, typename control_t, control_t default_value>
    class value_atomic_control
    {
    protected:
      union controlled_value
      {
        value_control_t        value_control_;
        std::atomic<control_t> control_;

        controlled_value() noexcept
          : control_{default_value}
        {}
        explicit controlled_value(value_control_t vc) noexcept
          : value_control_{vc}
        {}
        explicit controlled_value(control_t c) noexcept
          : control_{c}
        {}
      };

    protected:
      controlled_value value_;

    public:
      value_atomic_control() = default;

      explicit value_atomic_control(control_t ctrl) noexcept
        : value_{ctrl}
      {}
      explicit value_atomic_control(value_control_t val) noexcept
        : value_{val.control()}
      {}
      void store(value_control_t val) noexcept
      {
          value_.control_.store(val.control());
      }
      value_control_t load() const noexcept
      {
          return value_control_t{value_.control_.load()};
      }
      bool compare_exchange_weak(value_control_t& expected, value_control_t value) noexcept
      {
          return value_.control_.compare_exchange_weak(expected.control_ref(), value.control());
      }
      bool compare_exchange_strong(value_control_t& expected, value_control_t value) noexcept
      {
          return value_.control_.compare_exchange_strong(expected.control_ref(), value.control());
      }
      value_control_t fetch_add(value_control_t val) noexcept
      {
        return value_control_t{value_.control_.fetch_add(val.control())};
      }
    };

}} // namespace FASTER::core
