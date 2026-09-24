#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mar::okf {

struct YamlError {
    size_t line = 0;
    std::string message;
};

class Value;

class Mapping {
public:
    using Entry = std::pair<std::string, Value>;

    [[nodiscard]] size_t size() const { return entries_.size(); }
    [[nodiscard]] bool empty() const { return entries_.empty(); }

    [[nodiscard]] const Value* get(const std::string& key) const;
    [[nodiscard]] bool contains(const std::string& key) const { return get(key) != nullptr; }
    void insert(std::string key, Value value);
    void push(std::string key, Value value);

    [[nodiscard]] const std::vector<Entry>& entries() const { return entries_; }

private:
    std::vector<Entry> entries_;
};

class Value {
public:
    enum class Kind { Null, Bool, Int, Float, String, Sequence, Mapping };

    static Value null();
    static Value boolean(bool v);
    static Value string(std::string s);
    static Value mapping(Mapping m);
    static Value sequence(std::vector<Value> items);

    [[nodiscard]] Kind kind() const { return kind_; }
    [[nodiscard]] bool is_null() const { return kind_ == Kind::Null; }
    [[nodiscard]] const std::string* as_string() const;
    [[nodiscard]] std::optional<bool> as_bool() const;
    [[nodiscard]] const Mapping* as_mapping() const;
    [[nodiscard]] const std::vector<Value>* as_sequence() const;
    [[nodiscard]] std::optional<std::string> scalar_string() const;

    static Value parse(const std::string& text, YamlError* err = nullptr);
    [[nodiscard]] std::string to_yaml(int indent = 0) const;

private:
    Kind kind_ = Kind::Null;
    bool bool_ = false;
    std::string string_;
    std::vector<Value> sequence_;
    Mapping mapping_;

    Value() = default;
    explicit Value(Kind kind) : kind_(kind) {}
};

}  // namespace mar::okf
