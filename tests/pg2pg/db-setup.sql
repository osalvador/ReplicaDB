create table if not exists public.t_sink_views (
	schemaname name,
	viewname name,
	viewowner name,
	definition text,
	PRIMARY KEY (schemaname, viewname)
);
