"""billing baseline — squashed schema for the carved billing-service.

Creates the 16 billing tables (incl. the RANGE-partitioned ``cost_events``
with its monthly partitions, the ``create_cost_events_partition_for*``
helper functions, the ``pricing_id`` FK and all indexes / constraints).

The DDL is sourced verbatim from the authoritative live-schema dump
(``billing_schema_dump.sql``); pg_dump meta-commands (``\\restrict``,
``SET``, ``set_config``) are stripped. Old metering migrations 0001-0030 are
intentionally NOT replayed — this is a fresh squashed baseline.

Revision ID: 0001_billing_baseline
Revises:
Create Date: 2026-06-21
"""

from __future__ import annotations

import re

from alembic import op

revision = "0001_billing_baseline"
down_revision = None
branch_labels = None
depends_on = None


# --- Authoritative DDL (cleaned pg_dump of the live billing schema) ---------
_SCHEMA_SQL = r"""--
-- PostgreSQL database dump
--


-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg22.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg22.04+1)




--
-- Name: app_cost_rates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app_cost_rates (
    id uuid NOT NULL,
    app_key character varying(64) NOT NULL,
    cost_per_request_usd numeric(12,8) NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: audit_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_log (
    id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    actor_sub character varying(255),
    actor_role character varying(64),
    action character varying(64) NOT NULL,
    resource_type character varying(64) NOT NULL,
    resource_id character varying(255),
    tenant_id character varying(255),
    before jsonb,
    after jsonb,
    request_id character varying(64),
    tenant_name character varying(255)
);


--
-- Name: audit_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.audit_log_id_seq OWNED BY public.audit_log.id;


--
-- Name: budget_overage_alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.budget_overage_alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id character varying(255) NOT NULL,
    budget_id uuid NOT NULL,
    threshold_pct integer NOT NULL,
    period_start timestamp with time zone NOT NULL,
    dispatched_at timestamp with time zone DEFAULT now() NOT NULL,
    outcome character varying(32) DEFAULT 'ok'::character varying NOT NULL
);


--
-- Name: cost_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_daily (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid NOT NULL,
    service_key character varying(64) NOT NULL,
    provider character varying(64) NOT NULL,
    bucket_date date NOT NULL,
    total_input_tokens bigint NOT NULL,
    total_output_tokens bigint NOT NULL,
    total_cost_usd numeric(12,6) NOT NULL,
    request_count integer NOT NULL,
    subject_id uuid DEFAULT '00000000-0000-0000-0000-000000000003'::uuid NOT NULL,
    tenant_name character varying(255) NOT NULL,
    cost_source character varying(64) DEFAULT 'unknown'::character varying NOT NULL,
    served_model character varying(128) DEFAULT '__unknown__'::character varying NOT NULL,
    byo boolean DEFAULT false NOT NULL
);


--
-- Name: cost_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
)
PARTITION BY RANGE (created_at);


--
-- Name: cost_events_2026_04; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_04 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_05; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_05 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_06; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_06 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_07; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_07 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_08; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_08 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_09; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_09 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_10; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_10 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_11; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_11 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2026_12; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2026_12 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2027_01; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2027_01 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2027_02; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2027_02 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2027_03; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2027_03 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2027_04; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2027_04 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: cost_events_2027_05; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cost_events_2027_05 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cost_events_partitioned_id_not_null NOT NULL,
    tenant_id character varying(255) CONSTRAINT cost_events_partitioned_tenant_id_not_null NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying CONSTRAINT cost_events_partitioned_tenant_name_not_null NOT NULL,
    license_id uuid,
    subject_id uuid,
    app_id uuid,
    service_key character varying(64) CONSTRAINT cost_events_partitioned_service_key_not_null NOT NULL,
    provider character varying(64) CONSTRAINT cost_events_partitioned_provider_not_null NOT NULL,
    model character varying(128),
    served_model character varying(128),
    input_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_input_tokens_not_null NOT NULL,
    output_tokens integer DEFAULT 0 CONSTRAINT cost_events_partitioned_output_tokens_not_null NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_creation_input_tokens_not_null NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 CONSTRAINT cost_events_partitioned_cache_read_input_tokens_not_null NOT NULL,
    cost_usd numeric(12,8) DEFAULT 0,
    cost_usd_hint numeric(20,8),
    pricing_id bigint,
    provider_request_id character varying(255),
    usage_extra jsonb,
    latency_ms integer,
    metadata json,
    correlation_id uuid,
    cost_source character varying(64) DEFAULT 'unknown'::character varying CONSTRAINT cost_events_partitioned_cost_source_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT cost_events_partitioned_created_at_not_null NOT NULL
);


--
-- Name: markup_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.markup_rules (
    id bigint NOT NULL,
    tenant_id character varying(255),
    license_id uuid,
    service_key character varying(64),
    provider character varying(64),
    model character varying(128),
    markup_pct numeric(6,2),
    surcharge_input_per_1m_usd numeric(20,8),
    surcharge_output_per_1m_usd numeric(20,8),
    surcharge_cache_creation_per_1m_usd numeric(20,8),
    surcharge_cache_read_per_1m_usd numeric(20,8),
    surcharge_per_request_usd numeric(20,8),
    effective_from timestamp with time zone NOT NULL,
    effective_until timestamp with time zone,
    notes text
);


--
-- Name: markup_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.markup_rules_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: markup_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.markup_rules_id_seq OWNED BY public.markup_rules.id;


--
-- Name: metering_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metering_daily (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid NOT NULL,
    app_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    service_key character varying(64) NOT NULL,
    bucket_date date NOT NULL,
    request_count integer NOT NULL,
    denied_count integer NOT NULL,
    budget_denied_count integer DEFAULT 0 NOT NULL,
    subject_id uuid DEFAULT '00000000-0000-0000-0000-000000000003'::uuid NOT NULL,
    tenant_name character varying(255) NOT NULL
);


--
-- Name: metering_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metering_events (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid,
    app_id uuid,
    plan_id uuid,
    method character varying(10) NOT NULL,
    path character varying(512) NOT NULL,
    service_key character varying(64) NOT NULL,
    status character varying(20) NOT NULL,
    source_ip character varying(45),
    created_at timestamp with time zone NOT NULL,
    subject_id uuid,
    plan_code character varying(64),
    tenant_name character varying(255) NOT NULL
);


--
-- Name: metering_hourly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metering_hourly (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid NOT NULL,
    app_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    service_key character varying(64) NOT NULL,
    bucket_hour timestamp with time zone NOT NULL,
    request_count integer NOT NULL,
    denied_count integer NOT NULL,
    budget_denied_count integer DEFAULT 0 NOT NULL,
    subject_id uuid DEFAULT '00000000-0000-0000-0000-000000000003'::uuid NOT NULL,
    tenant_name character varying(255) NOT NULL
);


--
-- Name: metering_monthly_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.metering_monthly_usage (
    id uuid NOT NULL,
    license_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    month_start date NOT NULL,
    request_count integer NOT NULL,
    budget_denied_count integer DEFAULT 0 NOT NULL,
    subject_id uuid DEFAULT '00000000-0000-0000-0000-000000000003'::uuid NOT NULL,
    denied_count integer DEFAULT 0 NOT NULL
);


--
-- Name: provider_costs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_costs (
    id uuid NOT NULL,
    provider character varying(64) NOT NULL,
    input_per_1m_usd numeric(10,4) NOT NULL,
    output_per_1m_usd numeric(10,4) NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: provider_pricing; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_pricing (
    id bigint NOT NULL,
    provider character varying(64) NOT NULL,
    model character varying(128),
    cost_model character varying(32) NOT NULL,
    input_per_1m_usd numeric(20,8),
    output_per_1m_usd numeric(20,8),
    cache_creation_per_1m_usd numeric(20,8),
    cache_read_per_1m_usd numeric(20,8),
    per_request_usd numeric(20,8),
    monthly_fixed_cost_usd numeric(20,8),
    effective_from timestamp with time zone NOT NULL,
    effective_until timestamp with time zone,
    notes text,
    service_key character varying(64)
);


--
-- Name: provider_pricing_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.provider_pricing_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: provider_pricing_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.provider_pricing_id_seq OWNED BY public.provider_pricing.id;


--
-- Name: tenant_config; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_config (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    markup_pct numeric(6,2) NOT NULL,
    notes text,
    updated_at timestamp with time zone NOT NULL,
    tenant_name character varying(255) NOT NULL,
    billing_mode character varying(32) DEFAULT 'standard_markup'::character varying NOT NULL
);


--
-- Name: tenant_webhooks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tenant_webhooks (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id character varying(255) NOT NULL,
    url text NOT NULL,
    secret character varying(128) NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: token_budgets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.token_budgets (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid NOT NULL,
    budget_tokens bigint NOT NULL,
    period character varying(10) NOT NULL,
    includes_input boolean NOT NULL,
    includes_output boolean NOT NULL,
    active boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    starts_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    consumed_tokens numeric(20,6) DEFAULT 0 NOT NULL,
    source character varying(64) DEFAULT 'manual'::character varying NOT NULL,
    tenant_name character varying(255) DEFAULT ''::character varying NOT NULL
);


--
-- Name: token_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.token_usage (
    id uuid NOT NULL,
    tenant_id character varying(255) NOT NULL,
    license_id uuid NOT NULL,
    period_type character varying(10) NOT NULL,
    period_start date NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    total_tokens bigint NOT NULL,
    tenant_name character varying(255) NOT NULL
);


--
-- Name: cost_events_2026_04; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_04 FOR VALUES FROM ('2026-04-01 02:00:00+02') TO ('2026-05-01 02:00:00+02');


--
-- Name: cost_events_2026_05; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_05 FOR VALUES FROM ('2026-05-01 02:00:00+02') TO ('2026-06-01 02:00:00+02');


--
-- Name: cost_events_2026_06; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_06 FOR VALUES FROM ('2026-06-01 02:00:00+02') TO ('2026-07-01 02:00:00+02');


--
-- Name: cost_events_2026_07; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_07 FOR VALUES FROM ('2026-07-01 02:00:00+02') TO ('2026-08-01 02:00:00+02');


--
-- Name: cost_events_2026_08; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_08 FOR VALUES FROM ('2026-08-01 02:00:00+02') TO ('2026-09-01 02:00:00+02');


--
-- Name: cost_events_2026_09; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_09 FOR VALUES FROM ('2026-09-01 02:00:00+02') TO ('2026-10-01 02:00:00+02');


--
-- Name: cost_events_2026_10; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_10 FOR VALUES FROM ('2026-10-01 02:00:00+02') TO ('2026-11-01 01:00:00+01');


--
-- Name: cost_events_2026_11; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_11 FOR VALUES FROM ('2026-11-01 01:00:00+01') TO ('2026-12-01 01:00:00+01');


--
-- Name: cost_events_2026_12; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2026_12 FOR VALUES FROM ('2026-12-01 01:00:00+01') TO ('2027-01-01 01:00:00+01');


--
-- Name: cost_events_2027_01; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2027_01 FOR VALUES FROM ('2027-01-01 01:00:00+01') TO ('2027-02-01 01:00:00+01');


--
-- Name: cost_events_2027_02; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2027_02 FOR VALUES FROM ('2027-02-01 01:00:00+01') TO ('2027-03-01 01:00:00+01');


--
-- Name: cost_events_2027_03; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2027_03 FOR VALUES FROM ('2027-03-01 01:00:00+01') TO ('2027-04-01 02:00:00+02');


--
-- Name: cost_events_2027_04; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2027_04 FOR VALUES FROM ('2027-04-01 02:00:00+02') TO ('2027-05-01 02:00:00+02');


--
-- Name: cost_events_2027_05; Type: TABLE ATTACH; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events ATTACH PARTITION public.cost_events_2027_05 FOR VALUES FROM ('2027-05-01 02:00:00+02') TO ('2027-06-01 02:00:00+02');


--
-- Name: audit_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log ALTER COLUMN id SET DEFAULT nextval('public.audit_log_id_seq'::regclass);


--
-- Name: markup_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.markup_rules ALTER COLUMN id SET DEFAULT nextval('public.markup_rules_id_seq'::regclass);


--
-- Name: provider_pricing id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_pricing ALTER COLUMN id SET DEFAULT nextval('public.provider_pricing_id_seq'::regclass);


--
-- Name: app_cost_rates app_cost_rates_app_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_cost_rates
    ADD CONSTRAINT app_cost_rates_app_key_key UNIQUE (app_key);


--
-- Name: app_cost_rates app_cost_rates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_cost_rates
    ADD CONSTRAINT app_cost_rates_pkey PRIMARY KEY (id);


--
-- Name: audit_log audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);


--
-- Name: budget_overage_alerts budget_overage_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_overage_alerts
    ADD CONSTRAINT budget_overage_alerts_pkey PRIMARY KEY (id);


--
-- Name: cost_daily cost_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_daily
    ADD CONSTRAINT cost_daily_pkey PRIMARY KEY (id);


--
-- Name: cost_events cost_events_partitioned_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events
    ADD CONSTRAINT cost_events_partitioned_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_04 cost_events_2026_04_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_04
    ADD CONSTRAINT cost_events_2026_04_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_05 cost_events_2026_05_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_05
    ADD CONSTRAINT cost_events_2026_05_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_06 cost_events_2026_06_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_06
    ADD CONSTRAINT cost_events_2026_06_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_07 cost_events_2026_07_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_07
    ADD CONSTRAINT cost_events_2026_07_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_08 cost_events_2026_08_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_08
    ADD CONSTRAINT cost_events_2026_08_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_09 cost_events_2026_09_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_09
    ADD CONSTRAINT cost_events_2026_09_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_10 cost_events_2026_10_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_10
    ADD CONSTRAINT cost_events_2026_10_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_11 cost_events_2026_11_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_11
    ADD CONSTRAINT cost_events_2026_11_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2026_12 cost_events_2026_12_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2026_12
    ADD CONSTRAINT cost_events_2026_12_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2027_01 cost_events_2027_01_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2027_01
    ADD CONSTRAINT cost_events_2027_01_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2027_02 cost_events_2027_02_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2027_02
    ADD CONSTRAINT cost_events_2027_02_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2027_03 cost_events_2027_03_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2027_03
    ADD CONSTRAINT cost_events_2027_03_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2027_04 cost_events_2027_04_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2027_04
    ADD CONSTRAINT cost_events_2027_04_pkey PRIMARY KEY (id, created_at);


--
-- Name: cost_events_2027_05 cost_events_2027_05_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_events_2027_05
    ADD CONSTRAINT cost_events_2027_05_pkey PRIMARY KEY (id, created_at);


--
-- Name: markup_rules markup_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.markup_rules
    ADD CONSTRAINT markup_rules_pkey PRIMARY KEY (id);


--
-- Name: metering_daily metering_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_daily
    ADD CONSTRAINT metering_daily_pkey PRIMARY KEY (id);


--
-- Name: metering_events metering_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_events
    ADD CONSTRAINT metering_events_pkey PRIMARY KEY (id);


--
-- Name: metering_hourly metering_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_hourly
    ADD CONSTRAINT metering_hourly_pkey PRIMARY KEY (id);


--
-- Name: metering_monthly_usage metering_monthly_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_monthly_usage
    ADD CONSTRAINT metering_monthly_usage_pkey PRIMARY KEY (id);


--
-- Name: provider_costs provider_costs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_costs
    ADD CONSTRAINT provider_costs_pkey PRIMARY KEY (id);


--
-- Name: provider_costs provider_costs_provider_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_costs
    ADD CONSTRAINT provider_costs_provider_key UNIQUE (provider);


--
-- Name: provider_pricing provider_pricing_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_pricing
    ADD CONSTRAINT provider_pricing_pkey PRIMARY KEY (id);


--
-- Name: tenant_config tenant_config_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_config
    ADD CONSTRAINT tenant_config_pkey PRIMARY KEY (id);


--
-- Name: tenant_config tenant_config_tenant_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_config
    ADD CONSTRAINT tenant_config_tenant_id_key UNIQUE (tenant_id);


--
-- Name: tenant_webhooks tenant_webhooks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tenant_webhooks
    ADD CONSTRAINT tenant_webhooks_pkey PRIMARY KEY (id);


--
-- Name: token_budgets token_budgets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_budgets
    ADD CONSTRAINT token_budgets_pkey PRIMARY KEY (id);


--
-- Name: token_usage token_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_usage
    ADD CONSTRAINT token_usage_pkey PRIMARY KEY (id);


--
-- Name: budget_overage_alerts uq_budget_overage_dedupe; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.budget_overage_alerts
    ADD CONSTRAINT uq_budget_overage_dedupe UNIQUE (budget_id, threshold_pct, period_start);


--
-- Name: cost_daily uq_cost_daily; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cost_daily
    ADD CONSTRAINT uq_cost_daily UNIQUE (tenant_id, license_id, subject_id, service_key, provider, served_model, byo, cost_source, bucket_date);


--
-- Name: metering_daily uq_daily; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_daily
    ADD CONSTRAINT uq_daily UNIQUE (tenant_id, license_id, subject_id, app_id, plan_id, service_key, bucket_date);


--
-- Name: metering_hourly uq_hourly; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_hourly
    ADD CONSTRAINT uq_hourly UNIQUE (tenant_id, license_id, subject_id, app_id, plan_id, service_key, bucket_hour);


--
-- Name: metering_monthly_usage uq_monthly_usage; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.metering_monthly_usage
    ADD CONSTRAINT uq_monthly_usage UNIQUE (license_id, subject_id, plan_id, month_start);


--
-- Name: token_usage uq_token_usage; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_usage
    ADD CONSTRAINT uq_token_usage UNIQUE (tenant_id, license_id, period_type, period_start);


--
-- Name: ix_cost_events_correlation_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_cost_events_correlation_id ON ONLY public.cost_events USING btree (correlation_id);


--
-- Name: cost_events_2026_04_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_correlation_id_idx ON public.cost_events_2026_04 USING btree (correlation_id);


--
-- Name: ix_cost_events_source_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_cost_events_source_created ON ONLY public.cost_events USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_04_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_cost_source_created_at_idx ON public.cost_events_2026_04 USING btree (cost_source, created_at);


--
-- Name: ix_costs_license_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_costs_license_created ON ONLY public.cost_events USING btree (license_id, created_at);


--
-- Name: cost_events_2026_04_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_license_id_created_at_idx ON public.cost_events_2026_04 USING btree (license_id, created_at);


--
-- Name: ix_costs_provider_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_costs_provider_created ON ONLY public.cost_events USING btree (provider, created_at);


--
-- Name: cost_events_2026_04_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_provider_created_at_idx ON public.cost_events_2026_04 USING btree (provider, created_at);


--
-- Name: ix_costs_subject_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_costs_subject_created ON ONLY public.cost_events USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_04_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_subject_id_created_at_idx ON public.cost_events_2026_04 USING btree (subject_id, created_at);


--
-- Name: ix_costs_tenant_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_costs_tenant_created ON ONLY public.cost_events USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_04_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_tenant_id_created_at_idx ON public.cost_events_2026_04 USING btree (tenant_id, created_at);


--
-- Name: ix_cost_events_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_cost_events_tenant_name ON ONLY public.cost_events USING btree (tenant_name);


--
-- Name: cost_events_2026_04_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_04_tenant_name_idx ON public.cost_events_2026_04 USING btree (tenant_name);


--
-- Name: cost_events_2026_05_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_correlation_id_idx ON public.cost_events_2026_05 USING btree (correlation_id);


--
-- Name: cost_events_2026_05_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_cost_source_created_at_idx ON public.cost_events_2026_05 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_05_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_license_id_created_at_idx ON public.cost_events_2026_05 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_05_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_provider_created_at_idx ON public.cost_events_2026_05 USING btree (provider, created_at);


--
-- Name: cost_events_2026_05_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_subject_id_created_at_idx ON public.cost_events_2026_05 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_05_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_tenant_id_created_at_idx ON public.cost_events_2026_05 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_05_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_05_tenant_name_idx ON public.cost_events_2026_05 USING btree (tenant_name);


--
-- Name: cost_events_2026_06_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_correlation_id_idx ON public.cost_events_2026_06 USING btree (correlation_id);


--
-- Name: cost_events_2026_06_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_cost_source_created_at_idx ON public.cost_events_2026_06 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_06_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_license_id_created_at_idx ON public.cost_events_2026_06 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_06_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_provider_created_at_idx ON public.cost_events_2026_06 USING btree (provider, created_at);


--
-- Name: cost_events_2026_06_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_subject_id_created_at_idx ON public.cost_events_2026_06 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_06_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_tenant_id_created_at_idx ON public.cost_events_2026_06 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_06_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_06_tenant_name_idx ON public.cost_events_2026_06 USING btree (tenant_name);


--
-- Name: cost_events_2026_07_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_correlation_id_idx ON public.cost_events_2026_07 USING btree (correlation_id);


--
-- Name: cost_events_2026_07_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_cost_source_created_at_idx ON public.cost_events_2026_07 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_07_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_license_id_created_at_idx ON public.cost_events_2026_07 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_07_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_provider_created_at_idx ON public.cost_events_2026_07 USING btree (provider, created_at);


--
-- Name: cost_events_2026_07_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_subject_id_created_at_idx ON public.cost_events_2026_07 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_07_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_tenant_id_created_at_idx ON public.cost_events_2026_07 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_07_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_07_tenant_name_idx ON public.cost_events_2026_07 USING btree (tenant_name);


--
-- Name: cost_events_2026_08_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_correlation_id_idx ON public.cost_events_2026_08 USING btree (correlation_id);


--
-- Name: cost_events_2026_08_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_cost_source_created_at_idx ON public.cost_events_2026_08 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_08_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_license_id_created_at_idx ON public.cost_events_2026_08 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_08_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_provider_created_at_idx ON public.cost_events_2026_08 USING btree (provider, created_at);


--
-- Name: cost_events_2026_08_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_subject_id_created_at_idx ON public.cost_events_2026_08 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_08_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_tenant_id_created_at_idx ON public.cost_events_2026_08 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_08_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_08_tenant_name_idx ON public.cost_events_2026_08 USING btree (tenant_name);


--
-- Name: cost_events_2026_09_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_correlation_id_idx ON public.cost_events_2026_09 USING btree (correlation_id);


--
-- Name: cost_events_2026_09_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_cost_source_created_at_idx ON public.cost_events_2026_09 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_09_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_license_id_created_at_idx ON public.cost_events_2026_09 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_09_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_provider_created_at_idx ON public.cost_events_2026_09 USING btree (provider, created_at);


--
-- Name: cost_events_2026_09_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_subject_id_created_at_idx ON public.cost_events_2026_09 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_09_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_tenant_id_created_at_idx ON public.cost_events_2026_09 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_09_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_09_tenant_name_idx ON public.cost_events_2026_09 USING btree (tenant_name);


--
-- Name: cost_events_2026_10_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_correlation_id_idx ON public.cost_events_2026_10 USING btree (correlation_id);


--
-- Name: cost_events_2026_10_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_cost_source_created_at_idx ON public.cost_events_2026_10 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_10_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_license_id_created_at_idx ON public.cost_events_2026_10 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_10_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_provider_created_at_idx ON public.cost_events_2026_10 USING btree (provider, created_at);


--
-- Name: cost_events_2026_10_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_subject_id_created_at_idx ON public.cost_events_2026_10 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_10_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_tenant_id_created_at_idx ON public.cost_events_2026_10 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_10_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_10_tenant_name_idx ON public.cost_events_2026_10 USING btree (tenant_name);


--
-- Name: cost_events_2026_11_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_correlation_id_idx ON public.cost_events_2026_11 USING btree (correlation_id);


--
-- Name: cost_events_2026_11_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_cost_source_created_at_idx ON public.cost_events_2026_11 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_11_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_license_id_created_at_idx ON public.cost_events_2026_11 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_11_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_provider_created_at_idx ON public.cost_events_2026_11 USING btree (provider, created_at);


--
-- Name: cost_events_2026_11_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_subject_id_created_at_idx ON public.cost_events_2026_11 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_11_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_tenant_id_created_at_idx ON public.cost_events_2026_11 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_11_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_11_tenant_name_idx ON public.cost_events_2026_11 USING btree (tenant_name);


--
-- Name: cost_events_2026_12_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_correlation_id_idx ON public.cost_events_2026_12 USING btree (correlation_id);


--
-- Name: cost_events_2026_12_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_cost_source_created_at_idx ON public.cost_events_2026_12 USING btree (cost_source, created_at);


--
-- Name: cost_events_2026_12_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_license_id_created_at_idx ON public.cost_events_2026_12 USING btree (license_id, created_at);


--
-- Name: cost_events_2026_12_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_provider_created_at_idx ON public.cost_events_2026_12 USING btree (provider, created_at);


--
-- Name: cost_events_2026_12_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_subject_id_created_at_idx ON public.cost_events_2026_12 USING btree (subject_id, created_at);


--
-- Name: cost_events_2026_12_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_tenant_id_created_at_idx ON public.cost_events_2026_12 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2026_12_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2026_12_tenant_name_idx ON public.cost_events_2026_12 USING btree (tenant_name);


--
-- Name: cost_events_2027_01_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_correlation_id_idx ON public.cost_events_2027_01 USING btree (correlation_id);


--
-- Name: cost_events_2027_01_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_cost_source_created_at_idx ON public.cost_events_2027_01 USING btree (cost_source, created_at);


--
-- Name: cost_events_2027_01_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_license_id_created_at_idx ON public.cost_events_2027_01 USING btree (license_id, created_at);


--
-- Name: cost_events_2027_01_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_provider_created_at_idx ON public.cost_events_2027_01 USING btree (provider, created_at);


--
-- Name: cost_events_2027_01_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_subject_id_created_at_idx ON public.cost_events_2027_01 USING btree (subject_id, created_at);


--
-- Name: cost_events_2027_01_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_tenant_id_created_at_idx ON public.cost_events_2027_01 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2027_01_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_01_tenant_name_idx ON public.cost_events_2027_01 USING btree (tenant_name);


--
-- Name: cost_events_2027_02_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_correlation_id_idx ON public.cost_events_2027_02 USING btree (correlation_id);


--
-- Name: cost_events_2027_02_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_cost_source_created_at_idx ON public.cost_events_2027_02 USING btree (cost_source, created_at);


--
-- Name: cost_events_2027_02_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_license_id_created_at_idx ON public.cost_events_2027_02 USING btree (license_id, created_at);


--
-- Name: cost_events_2027_02_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_provider_created_at_idx ON public.cost_events_2027_02 USING btree (provider, created_at);


--
-- Name: cost_events_2027_02_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_subject_id_created_at_idx ON public.cost_events_2027_02 USING btree (subject_id, created_at);


--
-- Name: cost_events_2027_02_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_tenant_id_created_at_idx ON public.cost_events_2027_02 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2027_02_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_02_tenant_name_idx ON public.cost_events_2027_02 USING btree (tenant_name);


--
-- Name: cost_events_2027_03_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_correlation_id_idx ON public.cost_events_2027_03 USING btree (correlation_id);


--
-- Name: cost_events_2027_03_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_cost_source_created_at_idx ON public.cost_events_2027_03 USING btree (cost_source, created_at);


--
-- Name: cost_events_2027_03_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_license_id_created_at_idx ON public.cost_events_2027_03 USING btree (license_id, created_at);


--
-- Name: cost_events_2027_03_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_provider_created_at_idx ON public.cost_events_2027_03 USING btree (provider, created_at);


--
-- Name: cost_events_2027_03_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_subject_id_created_at_idx ON public.cost_events_2027_03 USING btree (subject_id, created_at);


--
-- Name: cost_events_2027_03_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_tenant_id_created_at_idx ON public.cost_events_2027_03 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2027_03_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_03_tenant_name_idx ON public.cost_events_2027_03 USING btree (tenant_name);


--
-- Name: cost_events_2027_04_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_correlation_id_idx ON public.cost_events_2027_04 USING btree (correlation_id);


--
-- Name: cost_events_2027_04_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_cost_source_created_at_idx ON public.cost_events_2027_04 USING btree (cost_source, created_at);


--
-- Name: cost_events_2027_04_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_license_id_created_at_idx ON public.cost_events_2027_04 USING btree (license_id, created_at);


--
-- Name: cost_events_2027_04_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_provider_created_at_idx ON public.cost_events_2027_04 USING btree (provider, created_at);


--
-- Name: cost_events_2027_04_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_subject_id_created_at_idx ON public.cost_events_2027_04 USING btree (subject_id, created_at);


--
-- Name: cost_events_2027_04_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_tenant_id_created_at_idx ON public.cost_events_2027_04 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2027_04_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_04_tenant_name_idx ON public.cost_events_2027_04 USING btree (tenant_name);


--
-- Name: cost_events_2027_05_correlation_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_correlation_id_idx ON public.cost_events_2027_05 USING btree (correlation_id);


--
-- Name: cost_events_2027_05_cost_source_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_cost_source_created_at_idx ON public.cost_events_2027_05 USING btree (cost_source, created_at);


--
-- Name: cost_events_2027_05_license_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_license_id_created_at_idx ON public.cost_events_2027_05 USING btree (license_id, created_at);


--
-- Name: cost_events_2027_05_provider_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_provider_created_at_idx ON public.cost_events_2027_05 USING btree (provider, created_at);


--
-- Name: cost_events_2027_05_subject_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_subject_id_created_at_idx ON public.cost_events_2027_05 USING btree (subject_id, created_at);


--
-- Name: cost_events_2027_05_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_tenant_id_created_at_idx ON public.cost_events_2027_05 USING btree (tenant_id, created_at);


--
-- Name: cost_events_2027_05_tenant_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cost_events_2027_05_tenant_name_idx ON public.cost_events_2027_05 USING btree (tenant_name);


--
-- Name: ix_audit_log_action_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_log_action_ts ON public.audit_log USING btree (action, ts DESC);


--
-- Name: ix_audit_log_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_log_tenant_name ON public.audit_log USING btree (tenant_name);


--
-- Name: ix_audit_log_tenant_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_log_tenant_ts ON public.audit_log USING btree (tenant_id, ts DESC);


--
-- Name: ix_audit_log_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_log_ts ON public.audit_log USING btree (ts DESC);


--
-- Name: ix_budget_overage_tenant_dispatched; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_budget_overage_tenant_dispatched ON public.budget_overage_alerts USING btree (tenant_id, dispatched_at);


--
-- Name: ix_cost_daily_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_cost_daily_tenant_name ON public.cost_daily USING btree (tenant_name);


--
-- Name: ix_events_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_events_created ON public.metering_events USING btree (created_at);


--
-- Name: ix_events_license_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_events_license_created ON public.metering_events USING btree (license_id, created_at);


--
-- Name: ix_events_subject_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_events_subject_created ON public.metering_events USING btree (subject_id, created_at);


--
-- Name: ix_events_tenant_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_events_tenant_created ON public.metering_events USING btree (tenant_id, created_at);


--
-- Name: ix_markuprules_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_markuprules_lookup ON public.markup_rules USING btree (COALESCE(tenant_id, ''::character varying), provider, model, effective_from DESC);


--
-- Name: ix_markuprules_open; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_markuprules_open ON public.markup_rules USING btree (COALESCE(tenant_id, ''::character varying), COALESCE((license_id)::text, ''::text), COALESCE(service_key, ''::character varying), COALESCE(provider, ''::character varying), COALESCE(model, ''::character varying)) WHERE (effective_until IS NULL);


--
-- Name: ix_metering_daily_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_metering_daily_tenant_name ON public.metering_daily USING btree (tenant_name);


--
-- Name: ix_metering_events_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_metering_events_tenant_name ON public.metering_events USING btree (tenant_name);


--
-- Name: ix_metering_hourly_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_metering_hourly_tenant_name ON public.metering_hourly USING btree (tenant_name);


--
-- Name: ix_providerpricing_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_providerpricing_lookup ON public.provider_pricing USING btree (provider, model, effective_from DESC);


--
-- Name: ix_providerpricing_open; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_providerpricing_open ON public.provider_pricing USING btree (provider, COALESCE(model, ''::character varying), COALESCE(service_key, ''::character varying)) WHERE (effective_until IS NULL);


--
-- Name: ix_tenant_config_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tenant_config_tenant_name ON public.tenant_config USING btree (tenant_name);


--
-- Name: ix_tenant_webhooks_tenant_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tenant_webhooks_tenant_active ON public.tenant_webhooks USING btree (tenant_id, active);


--
-- Name: ix_token_budgets_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_token_budgets_tenant_name ON public.token_budgets USING btree (tenant_name);


--
-- Name: ix_token_usage_tenant_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_token_usage_tenant_name ON public.token_usage USING btree (tenant_name);


--
-- Name: ix_tokenbudget_active_drain; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_tokenbudget_active_drain ON public.token_budgets USING btree (tenant_id, active, priority, expires_at);


--
-- Name: cost_events_2026_04_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_04_correlation_id_idx;


--
-- Name: cost_events_2026_04_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_04_cost_source_created_at_idx;


--
-- Name: cost_events_2026_04_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_04_license_id_created_at_idx;


--
-- Name: cost_events_2026_04_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_04_pkey;


--
-- Name: cost_events_2026_04_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_04_provider_created_at_idx;


--
-- Name: cost_events_2026_04_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_04_subject_id_created_at_idx;


--
-- Name: cost_events_2026_04_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_04_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_04_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_04_tenant_name_idx;


--
-- Name: cost_events_2026_05_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_05_correlation_id_idx;


--
-- Name: cost_events_2026_05_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_05_cost_source_created_at_idx;


--
-- Name: cost_events_2026_05_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_05_license_id_created_at_idx;


--
-- Name: cost_events_2026_05_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_05_pkey;


--
-- Name: cost_events_2026_05_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_05_provider_created_at_idx;


--
-- Name: cost_events_2026_05_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_05_subject_id_created_at_idx;


--
-- Name: cost_events_2026_05_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_05_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_05_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_05_tenant_name_idx;


--
-- Name: cost_events_2026_06_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_06_correlation_id_idx;


--
-- Name: cost_events_2026_06_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_06_cost_source_created_at_idx;


--
-- Name: cost_events_2026_06_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_06_license_id_created_at_idx;


--
-- Name: cost_events_2026_06_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_06_pkey;


--
-- Name: cost_events_2026_06_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_06_provider_created_at_idx;


--
-- Name: cost_events_2026_06_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_06_subject_id_created_at_idx;


--
-- Name: cost_events_2026_06_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_06_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_06_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_06_tenant_name_idx;


--
-- Name: cost_events_2026_07_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_07_correlation_id_idx;


--
-- Name: cost_events_2026_07_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_07_cost_source_created_at_idx;


--
-- Name: cost_events_2026_07_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_07_license_id_created_at_idx;


--
-- Name: cost_events_2026_07_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_07_pkey;


--
-- Name: cost_events_2026_07_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_07_provider_created_at_idx;


--
-- Name: cost_events_2026_07_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_07_subject_id_created_at_idx;


--
-- Name: cost_events_2026_07_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_07_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_07_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_07_tenant_name_idx;


--
-- Name: cost_events_2026_08_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_08_correlation_id_idx;


--
-- Name: cost_events_2026_08_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_08_cost_source_created_at_idx;


--
-- Name: cost_events_2026_08_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_08_license_id_created_at_idx;


--
-- Name: cost_events_2026_08_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_08_pkey;


--
-- Name: cost_events_2026_08_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_08_provider_created_at_idx;


--
-- Name: cost_events_2026_08_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_08_subject_id_created_at_idx;


--
-- Name: cost_events_2026_08_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_08_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_08_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_08_tenant_name_idx;


--
-- Name: cost_events_2026_09_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_09_correlation_id_idx;


--
-- Name: cost_events_2026_09_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_09_cost_source_created_at_idx;


--
-- Name: cost_events_2026_09_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_09_license_id_created_at_idx;


--
-- Name: cost_events_2026_09_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_09_pkey;


--
-- Name: cost_events_2026_09_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_09_provider_created_at_idx;


--
-- Name: cost_events_2026_09_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_09_subject_id_created_at_idx;


--
-- Name: cost_events_2026_09_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_09_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_09_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_09_tenant_name_idx;


--
-- Name: cost_events_2026_10_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_10_correlation_id_idx;


--
-- Name: cost_events_2026_10_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_10_cost_source_created_at_idx;


--
-- Name: cost_events_2026_10_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_10_license_id_created_at_idx;


--
-- Name: cost_events_2026_10_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_10_pkey;


--
-- Name: cost_events_2026_10_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_10_provider_created_at_idx;


--
-- Name: cost_events_2026_10_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_10_subject_id_created_at_idx;


--
-- Name: cost_events_2026_10_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_10_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_10_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_10_tenant_name_idx;


--
-- Name: cost_events_2026_11_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_11_correlation_id_idx;


--
-- Name: cost_events_2026_11_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_11_cost_source_created_at_idx;


--
-- Name: cost_events_2026_11_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_11_license_id_created_at_idx;


--
-- Name: cost_events_2026_11_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_11_pkey;


--
-- Name: cost_events_2026_11_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_11_provider_created_at_idx;


--
-- Name: cost_events_2026_11_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_11_subject_id_created_at_idx;


--
-- Name: cost_events_2026_11_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_11_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_11_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_11_tenant_name_idx;


--
-- Name: cost_events_2026_12_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2026_12_correlation_id_idx;


--
-- Name: cost_events_2026_12_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2026_12_cost_source_created_at_idx;


--
-- Name: cost_events_2026_12_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2026_12_license_id_created_at_idx;


--
-- Name: cost_events_2026_12_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2026_12_pkey;


--
-- Name: cost_events_2026_12_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2026_12_provider_created_at_idx;


--
-- Name: cost_events_2026_12_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2026_12_subject_id_created_at_idx;


--
-- Name: cost_events_2026_12_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2026_12_tenant_id_created_at_idx;


--
-- Name: cost_events_2026_12_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2026_12_tenant_name_idx;


--
-- Name: cost_events_2027_01_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2027_01_correlation_id_idx;


--
-- Name: cost_events_2027_01_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2027_01_cost_source_created_at_idx;


--
-- Name: cost_events_2027_01_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2027_01_license_id_created_at_idx;


--
-- Name: cost_events_2027_01_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2027_01_pkey;


--
-- Name: cost_events_2027_01_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2027_01_provider_created_at_idx;


--
-- Name: cost_events_2027_01_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2027_01_subject_id_created_at_idx;


--
-- Name: cost_events_2027_01_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2027_01_tenant_id_created_at_idx;


--
-- Name: cost_events_2027_01_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2027_01_tenant_name_idx;


--
-- Name: cost_events_2027_02_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2027_02_correlation_id_idx;


--
-- Name: cost_events_2027_02_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2027_02_cost_source_created_at_idx;


--
-- Name: cost_events_2027_02_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2027_02_license_id_created_at_idx;


--
-- Name: cost_events_2027_02_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2027_02_pkey;


--
-- Name: cost_events_2027_02_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2027_02_provider_created_at_idx;


--
-- Name: cost_events_2027_02_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2027_02_subject_id_created_at_idx;


--
-- Name: cost_events_2027_02_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2027_02_tenant_id_created_at_idx;


--
-- Name: cost_events_2027_02_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2027_02_tenant_name_idx;


--
-- Name: cost_events_2027_03_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2027_03_correlation_id_idx;


--
-- Name: cost_events_2027_03_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2027_03_cost_source_created_at_idx;


--
-- Name: cost_events_2027_03_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2027_03_license_id_created_at_idx;


--
-- Name: cost_events_2027_03_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2027_03_pkey;


--
-- Name: cost_events_2027_03_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2027_03_provider_created_at_idx;


--
-- Name: cost_events_2027_03_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2027_03_subject_id_created_at_idx;


--
-- Name: cost_events_2027_03_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2027_03_tenant_id_created_at_idx;


--
-- Name: cost_events_2027_03_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2027_03_tenant_name_idx;


--
-- Name: cost_events_2027_04_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2027_04_correlation_id_idx;


--
-- Name: cost_events_2027_04_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2027_04_cost_source_created_at_idx;


--
-- Name: cost_events_2027_04_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2027_04_license_id_created_at_idx;


--
-- Name: cost_events_2027_04_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2027_04_pkey;


--
-- Name: cost_events_2027_04_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2027_04_provider_created_at_idx;


--
-- Name: cost_events_2027_04_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2027_04_subject_id_created_at_idx;


--
-- Name: cost_events_2027_04_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2027_04_tenant_id_created_at_idx;


--
-- Name: cost_events_2027_04_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2027_04_tenant_name_idx;


--
-- Name: cost_events_2027_05_correlation_id_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_correlation_id ATTACH PARTITION public.cost_events_2027_05_correlation_id_idx;


--
-- Name: cost_events_2027_05_cost_source_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_source_created ATTACH PARTITION public.cost_events_2027_05_cost_source_created_at_idx;


--
-- Name: cost_events_2027_05_license_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_license_created ATTACH PARTITION public.cost_events_2027_05_license_id_created_at_idx;


--
-- Name: cost_events_2027_05_pkey; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.cost_events_partitioned_pkey ATTACH PARTITION public.cost_events_2027_05_pkey;


--
-- Name: cost_events_2027_05_provider_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_provider_created ATTACH PARTITION public.cost_events_2027_05_provider_created_at_idx;


--
-- Name: cost_events_2027_05_subject_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_subject_created ATTACH PARTITION public.cost_events_2027_05_subject_id_created_at_idx;


--
-- Name: cost_events_2027_05_tenant_id_created_at_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_costs_tenant_created ATTACH PARTITION public.cost_events_2027_05_tenant_id_created_at_idx;


--
-- Name: cost_events_2027_05_tenant_name_idx; Type: INDEX ATTACH; Schema: public; Owner: -
--

ALTER INDEX public.ix_cost_events_tenant_name ATTACH PARTITION public.cost_events_2027_05_tenant_name_idx;


--
-- Name: cost_events cost_events_pricing_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.cost_events
    ADD CONSTRAINT cost_events_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.provider_pricing(id) ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--


--- PARTITION FUNCTION ---
CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for(p_year integer, p_month integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            partition_name TEXT;
            from_date TEXT;
            to_date TEXT;
        BEGIN
            partition_name := format('cost_events_%s_%s',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            from_date := format('%s-%s-01',
                lpad(p_year::text, 4, '0'),
                lpad(p_month::text, 2, '0'));
            -- Compute the next month's start by adding 1 month then truncating.
            to_date := to_char(
                (from_date::timestamp + interval '1 month')::date,
                'YYYY-MM-01'
            );

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF cost_events
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name, from_date, to_date
            );
            RETURN partition_name;
        END
        $function$;

CREATE OR REPLACE FUNCTION public.create_cost_events_partition_for_next_month()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
        DECLARE
            target_date DATE;
        BEGIN
            target_date := (date_trunc('month', now()) + interval '1 month')::date;
            RETURN create_cost_events_partition_for(
                EXTRACT(YEAR FROM target_date)::int,
                EXTRACT(MONTH FROM target_date)::int
            );
        END
        $function$;
"""


def _split_sql_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL script into individual statements.

    Dollar-quote aware (``$tag$ ... $tag$``) and single-quote aware so the
    ``;`` inside plpgsql function bodies / string literals does not terminate
    a statement prematurely. ``--`` line comments are skipped when scanning
    for quotes/terminators.
    """
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    dollar_tag: str | None = None
    while i < n:
        ch = sql[i]
        # line comment (only outside any quote)
        if not in_single and dollar_tag is None and ch == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i)
            if nl == -1:
                buf.append(sql[i:])
                i = n
            else:
                buf.append(sql[i:nl])
                i = nl
            continue
        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                buf.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue
            buf.append(ch)
            i += 1
            continue
        if in_single:
            buf.append(ch)
            if ch == "'":
                in_single = False
            i += 1
            continue
        # not in any quote
        if ch == "'":
            in_single = True
            buf.append(ch)
            i += 1
            continue
        m = re.match(r"\$[A-Za-z_]*\$", sql[i:])
        if m:
            dollar_tag = m.group(0)
            buf.append(dollar_tag)
            i += len(dollar_tag)
            continue
        if ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


# 16 billing base tables (partitions of cost_events drop with the parent via
# CASCADE; sequences owned by table columns drop with the table).
_TABLES = [
    "app_cost_rates",
    "audit_log",
    "budget_overage_alerts",
    "cost_daily",
    "cost_events",
    "markup_rules",
    "metering_daily",
    "metering_events",
    "metering_hourly",
    "metering_monthly_usage",
    "provider_costs",
    "provider_pricing",
    "tenant_config",
    "tenant_webhooks",
    "token_budgets",
    "token_usage",
]


def upgrade() -> None:
    for statement in _split_sql_statements(_SCHEMA_SQL):
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS public.create_cost_events_partition_for_next_month();")
    op.execute("DROP FUNCTION IF EXISTS public.create_cost_events_partition_for(integer, integer);")
    for table in _TABLES:
        op.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE;")
