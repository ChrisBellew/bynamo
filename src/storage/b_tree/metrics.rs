use prometheus::{
    exponential_buckets, linear_buckets, Histogram, HistogramOpts, IntCounter, IntGauge, Opts,
    Registry,
};

#[derive(Clone)]
pub struct BTreeMetrics {
    pub adds_histogram: Histogram,
    pub add_root_lock_wait_histogram: Histogram,
    pub add_node_lock_wait_histogram: Histogram,
    pub add_get_wait_histogram: Histogram,
    pub add_persist_wait_histogram: Histogram,
    pub add_insert_wait_histogram: Histogram,
    pub add_remaining_histogram: Histogram,
    pub add_traversals_histogram: Histogram,
    pub add_root_lock_waiters_gauge: IntGauge,
    pub add_node_lock_waiters_gauge: IntGauge,
    pub add_get_waiters_gauge: IntGauge,
    pub add_persist_waiters_gauge: IntGauge,
    pub add_insert_waiters_gauge: IntGauge,
    pub add_workers_gauge: IntGauge,
    pub add_bytes_counter: IntCounter,
    pub write_bytes_counter: IntCounter,
    pub node_count_gauge: IntGauge,
    pub depth_gauge: IntGauge,

    // Node store
    pub writes_histogram: Histogram,
    pub buffer_size_gauge: IntGauge,
    pub flush_size_histogram: Histogram,
    pub flush_sort_duration_histogram: Histogram,
    pub flush_get_duration_histogram: Histogram,
    pub flush_duration_histogram: Histogram,
    pub seek_size_histogram: Histogram,
}

impl BTreeMetrics {
    pub fn register(tree_id: usize, registry: &Registry) -> BTreeMetrics {
        let adds_histogram = Histogram::with_opts(
            HistogramOpts::new("btree_adds_histogram", "Btree add durations in seconds")
                .const_label("tree", tree_id.to_string())
                .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry.register(Box::new(adds_histogram.clone())).unwrap();

        let add_root_lock_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_root_lock_wait_histogram",
                "Btree add root lock wait durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_root_lock_wait_histogram.clone()))
            .unwrap();

        let add_node_lock_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_node_lock_wait_histogram",
                "Btree add node lock wait durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_node_lock_wait_histogram.clone()))
            .unwrap();

        let add_get_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_get_wait_histogram",
                "Btree add get wait durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_get_wait_histogram.clone()))
            .unwrap();

        let add_persist_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_persist_wait_histogram",
                "Btree add persist wait durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_persist_wait_histogram.clone()))
            .unwrap();

        let add_insert_wait_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_insert_wait_histogram",
                "Btree add insert wait durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_insert_wait_histogram.clone()))
            .unwrap();

        let add_remaining_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_remaining_histogram",
                "Btree add remaining durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_remaining_histogram.clone()))
            .unwrap();

        let add_traversals_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "btree_add_traversals_histogram",
                "Btree add traversals depths",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(linear_buckets(1.0, 1.0, 30).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(add_traversals_histogram.clone()))
            .unwrap();

        let add_root_lock_waiters_gauge = IntGauge::with_opts(
            Opts::new(
                "btree_add_root_lock_waiters_gauge",
                "Btree add root lock waiters gauge",
            )
            .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_root_lock_waiters_gauge.clone()))
            .unwrap();

        let add_node_lock_waiters_gauge = IntGauge::with_opts(
            Opts::new(
                "btree_add_node_lock_waiters_gauge",
                "Btree add node lock waiters gauge",
            )
            .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_node_lock_waiters_gauge.clone()))
            .unwrap();

        let add_get_waiters_gauge = IntGauge::with_opts(
            Opts::new("btree_add_get_waiters_gauge", "Btree add get waiters gauge")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_get_waiters_gauge.clone()))
            .unwrap();

        let add_persist_waiters_gauge = IntGauge::with_opts(
            Opts::new(
                "btree_add_persist_waiters_gauge",
                "Btree add persist waiters gauge",
            )
            .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_persist_waiters_gauge.clone()))
            .unwrap();

        let add_insert_waiters_gauge = IntGauge::with_opts(
            Opts::new(
                "btree_add_insert_waiters_gauge",
                "Btree add insert waiters gauge",
            )
            .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_insert_waiters_gauge.clone()))
            .unwrap();

        let add_workers_gauge = IntGauge::with_opts(
            Opts::new("btree_add_workers_gauge", "Btree add workers gauge")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_workers_gauge.clone()))
            .unwrap();

        let add_bytes_counter = IntCounter::with_opts(
            Opts::new("btree_add_bytes_counter", "Btree add bytes counter")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(add_bytes_counter.clone()))
            .unwrap();

        let write_bytes_counter = IntCounter::with_opts(
            Opts::new("btree_write_bytes_counter", "Btree write bytes counter")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(write_bytes_counter.clone()))
            .unwrap();

        let node_count_gauge = IntGauge::with_opts(
            Opts::new("btree_node_count_gauge", "Btree node count gauge")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(node_count_gauge.clone()))
            .unwrap();

        let depth_gauge = IntGauge::with_opts(
            Opts::new("btree_depth_gauge", "Btree depth gauge")
                .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry.register(Box::new(depth_gauge.clone())).unwrap();

        // ----------------- //
        // NODE STORE        //
        // ----------------- //

        let writes_histogram = Histogram::with_opts(
            HistogramOpts::new("store_writes_histogram", "Write durations in seconds")
                .const_label("tree", tree_id.to_string())
                .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(writes_histogram.clone()))
            .unwrap();

        let buffer_size_gauge = IntGauge::with_opts(
            Opts::new(
                "node_store_buffer_size",
                "Size of node store write buffer in bytes",
            )
            .const_label("tree", tree_id.to_string()),
        )
        .unwrap();
        registry
            .register(Box::new(buffer_size_gauge.clone()))
            .unwrap();

        let flush_size_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "node_store_flush_size_histogram",
                "Node store flush size in bytes",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(flush_size_histogram.clone()))
            .unwrap();

        let flush_sort_duration_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "node_store_flush_sort_duration_histogram",
                "Node store flush sort durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(flush_sort_duration_histogram.clone()))
            .unwrap();

        let flush_get_duration_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "node_store_flush_get_duration_histogram",
                "Node store flush get durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(flush_get_duration_histogram.clone()))
            .unwrap();

        let flush_duration_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "node_store_flush_duration_histogram",
                "Node store flush durations in seconds",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(10f64.powf(-9.0), 3.0, 22).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(flush_duration_histogram.clone()))
            .unwrap();

        let seek_size_histogram = Histogram::with_opts(
            HistogramOpts::new(
                "node_store_seek_size_histogram",
                "Node store seek size in bytes",
            )
            .const_label("tree", tree_id.to_string())
            .buckets(exponential_buckets(20.0, 3.0, 15).unwrap()),
        )
        .unwrap();
        registry
            .register(Box::new(seek_size_histogram.clone()))
            .unwrap();

        BTreeMetrics {
            adds_histogram,
            add_root_lock_wait_histogram,
            add_node_lock_wait_histogram,
            add_get_wait_histogram,
            add_persist_wait_histogram,
            add_insert_wait_histogram,
            add_remaining_histogram,
            add_traversals_histogram,
            add_root_lock_waiters_gauge,
            add_node_lock_waiters_gauge,
            add_get_waiters_gauge,
            add_persist_waiters_gauge,
            add_insert_waiters_gauge,
            add_workers_gauge,
            add_bytes_counter,
            write_bytes_counter,
            node_count_gauge,
            depth_gauge,
            writes_histogram,
            buffer_size_gauge,
            flush_size_histogram,
            flush_sort_duration_histogram,
            flush_get_duration_histogram,
            flush_duration_histogram,
            seek_size_histogram,
        }
    }
}
