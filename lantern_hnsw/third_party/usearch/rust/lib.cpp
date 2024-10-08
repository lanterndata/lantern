#include "lib.hpp"
#include "usearch/rust/lib.rs.h"
#include "usearch/storage.hpp"

using namespace unum::usearch;
using namespace unum;

using index_dense_t = NativeIndex::index_dense_t;
using index_t = index_dense_t;

using add_result_t = typename index_t::add_result_t;
using search_result_t = typename index_t::search_result_t;
using labeling_result_t = typename index_t::labeling_result_t;
using vector_key_t = typename index_dense_t::vector_key_t;

template <typename scalar_at> Matches search_(index_dense_t& index, scalar_at const* vec, size_t count) {
    Matches matches;
    matches.keys.reserve(count);
    matches.distances.reserve(count);
    for (size_t i = 0; i != count; ++i)
        matches.keys.push_back(0), matches.distances.push_back(0);
    search_result_t result = index.search(vec, count);
    result.error.raise();
    count = result.dump_to(matches.keys.data(), matches.distances.data());
    matches.keys.truncate(count);
    matches.distances.truncate(count);
    return matches;
}

NativeIndex::NativeIndex(std::unique_ptr<index_t> index) : index_(std::move(index)) {}

// clang-format off
void NativeIndex::add_i8(vector_key_t key, rust::Slice<int8_t const> vec) const { index_->add(key, vec.data()).error.raise(); }
void NativeIndex::add_f16(vector_key_t key, rust::Slice<uint16_t const> vec) const { index_->add(key, (f16_t const*)vec.data()).error.raise(); }
void NativeIndex::add_f32(vector_key_t key, rust::Slice<float const> vec) const { index_->add(key, vec.data()).error.raise(); }
void NativeIndex::add_f64(vector_key_t key, rust::Slice<double const> vec) const { index_->add(key, vec.data()).error.raise(); }

Matches NativeIndex::search_i8(rust::Slice<int8_t const> vec, size_t count) const { return search_(*index_, vec.data(), count); }
Matches NativeIndex::search_f16(rust::Slice<uint16_t const> vec, size_t count) const { return search_(*index_, (f16_t const*)vec.data(), count); }
Matches NativeIndex::search_f32(rust::Slice<float const> vec, size_t count) const { return search_(*index_, vec.data(), count); }
Matches NativeIndex::search_f64(rust::Slice<double const> vec, size_t count) const { return search_(*index_, vec.data(), count); }

size_t NativeIndex::get_i8(vector_key_t key, rust::Slice<int8_t> vec) const { if (vec.size() % dimensions()) throw std::invalid_argument("Vector length must be a multiple of index dimensionality"); return index_->get(key, vec.data(), vec.size() / dimensions()); }
size_t NativeIndex::get_f16(vector_key_t key, rust::Slice<uint16_t> vec) const { if (vec.size() % dimensions()) throw std::invalid_argument("Vector length must be a multiple of index dimensionality"); return index_->get(key, (f16_t*)vec.data(), vec.size() / dimensions()); }
size_t NativeIndex::get_f32(vector_key_t key, rust::Slice<float> vec) const { if (vec.size() % dimensions()) throw std::invalid_argument("Vector length must be a multiple of index dimensionality"); return index_->get(key, vec.data(), vec.size() / dimensions()); }
size_t NativeIndex::get_f64(vector_key_t key, rust::Slice<double> vec) const { if (vec.size() % dimensions()) throw std::invalid_argument("Vector length must be a multiple of index dimensionality"); return index_->get(key, vec.data(), vec.size() / dimensions()); }
// clang-format on

size_t NativeIndex::expansion_add() const { return index_->expansion_add(); }
size_t NativeIndex::expansion_search() const { return index_->expansion_search(); }
void NativeIndex::change_expansion_add(size_t n) const { index_->change_expansion_add(n); }
void NativeIndex::change_expansion_search(size_t n) const { index_->change_expansion_search(n); }

size_t NativeIndex::remove(vector_key_t key) const {
    labeling_result_t result = index_->remove(key);
    result.error.raise();
    return result.completed;
}

size_t NativeIndex::rename(vector_key_t from, vector_key_t to) const {
    labeling_result_t result = index_->rename(from, to);
    result.error.raise();
    return result.completed;
}

size_t NativeIndex::count(vector_key_t key) const { return index_->count(key); }

void NativeIndex::reserve(size_t capacity) const { index_->reserve(capacity); }

size_t NativeIndex::dimensions() const { return index_->dimensions(); }
size_t NativeIndex::connectivity() const { return index_->connectivity(); }
size_t NativeIndex::size() const { return index_->size(); }
size_t NativeIndex::capacity() const { return index_->capacity(); }
size_t NativeIndex::serialized_length() const { return index_->serialized_length(); }

void NativeIndex::save(rust::Str path) const { index_->save(output_file_t(std::string(path).c_str())).error.raise(); }
void NativeIndex::load(rust::Str path) const { index_->load(input_file_t(std::string(path).c_str())).error.raise(); }
void NativeIndex::view(rust::Str path) const {
    index_->view(memory_mapped_file_t(std::string(path).c_str())).error.raise();
}

void NativeIndex::reset() const { index_->reset(); }
size_t NativeIndex::memory_usage() const { return index_->memory_usage(); }

void NativeIndex::save_to_buffer(rust::Slice<uint8_t> buffer) const {
    index_->save(memory_mapped_file_t((byte_t*)buffer.data(), buffer.size())).error.raise();
}

void NativeIndex::load_from_buffer(rust::Slice<uint8_t const> buffer) const {
    index_->load(memory_mapped_file_t((byte_t*)buffer.data(), buffer.size())).error.raise();
}

void NativeIndex::view_from_buffer(rust::Slice<uint8_t const> buffer) const {
    index_->view(memory_mapped_file_t((byte_t*)buffer.data(), buffer.size())).error.raise();
}

std::unique_ptr<NativeIndex> wrap(index_t&& index) {
    std::unique_ptr<index_t> punned_ptr;
    punned_ptr.reset(new index_t(std::move(index)));
    std::unique_ptr<NativeIndex> result;
    result.reset(new NativeIndex(std::move(punned_ptr)));
    return result;
}

metric_kind_t rust_to_cpp_metric(MetricKind value) {
    switch (value) {
    case MetricKind::IP: return metric_kind_t::ip_k;
    case MetricKind::L2sq: return metric_kind_t::l2sq_k;
    case MetricKind::Cos: return metric_kind_t::cos_k;
    case MetricKind::Pearson: return metric_kind_t::pearson_k;
    case MetricKind::Haversine: return metric_kind_t::haversine_k;
    case MetricKind::Divergence: return metric_kind_t::divergence_k;
    case MetricKind::Hamming: return metric_kind_t::hamming_k;
    case MetricKind::Tanimoto: return metric_kind_t::tanimoto_k;
    case MetricKind::Sorensen: return metric_kind_t::sorensen_k;
    default: return metric_kind_t::unknown_k;
    }
}

scalar_kind_t rust_to_cpp_scalar(ScalarKind value) {
    switch (value) {
    case ScalarKind::I8: return scalar_kind_t::i8_k;
    case ScalarKind::F16: return scalar_kind_t::f16_k;
    case ScalarKind::F32: return scalar_kind_t::f32_k;
    case ScalarKind::F64: return scalar_kind_t::f64_k;
    case ScalarKind::B1: return scalar_kind_t::b1x8_k;
    default: return scalar_kind_t::unknown_k;
    }
}

std::unique_ptr<NativeIndex> new_native_index(IndexOptions const& options) {
    metric_kind_t metric_kind = rust_to_cpp_metric(options.metric);
    scalar_kind_t scalar_kind = rust_to_cpp_scalar(options.quantization);
    metric_punned_t metric(options.dimensions, metric_kind, scalar_kind);
    index_dense_config_t config(options.connectivity, options.expansion_add, options.expansion_search);
    config.multi = options.multi;
    storage_options opts{};
    opts.dimensions = options.dimensions;
    opts.pq = options.pq_output;
    assert(options.pq_output == options.pq_construction);
    opts.num_centroids = options.num_centroids;
    opts.num_subvectors = options.num_subvectors;
    opts.scalar_bits = bits_per_scalar(scalar_kind);
    return wrap(index_t::make(metric, opts, options.num_threads, config, options.codebook));
}
