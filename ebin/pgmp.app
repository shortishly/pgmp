{application, 'pgmp', [
	{description, "PostgreSQL Message Protocol"},
	{vsn, "0.22.0-36-g7c64cc5"},
	{modules, ['pgmp','pgmp_app','pgmp_binary','pgmp_calendar','pgmp_codec','pgmp_config','pgmp_connection','pgmp_connection_sync','pgmp_data_row','pgmp_db','pgmp_db_sup','pgmp_dbs','pgmp_dbs_sup','pgmp_error_notice_fields','pgmp_geo','pgmp_identity','pgmp_int_sup','pgmp_lsn','pgmp_message_tags','pgmp_mm','pgmp_mm_auth_md5','pgmp_mm_auth_sasl','pgmp_mm_bootstrap','pgmp_mm_common','pgmp_mm_equery','pgmp_mm_rep_log','pgmp_mm_rep_phy','pgmp_mm_squery','pgmp_pool_connection_sup','pgmp_pool_sup','pgmp_rep_log','pgmp_rep_log_ets','pgmp_rep_log_ets_backfill','pgmp_rep_log_ets_common','pgmp_rep_log_ets_snapshot','pgmp_rep_log_stream_sup','pgmp_rep_log_sup','pgmp_rep_sup','pgmp_scram','pgmp_socket','pgmp_statem','pgmp_sup','pgmp_telemetry','pgmp_telemetry_logger','pgmp_tsquery','pgmp_tsvector','pgmp_types','pgmp_uri','pgmp_util']},
	{registered, [pgmp_sup]},
	{applications, [kernel,stdlib,backoff,envy,phrase,recon,telemetry]},
	{optional_applications, []},
	{mod, {pgmp_app, []}},
	{env, []}
]}.