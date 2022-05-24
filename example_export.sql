--
-- PostgreSQL database dump
--

-- Dumped from database version 11.16
-- Dumped by pg_dump version 11.16

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: outschool_learner_pii; Type: TABLE; Schema: public; Owner: jmh
--

CREATE TABLE public.outschool_learner_pii (
    uid uuid,
    email_address text,
    personal_info text,
    secret_token uuid
);


ALTER TABLE public.outschool_learner_pii OWNER TO jmh;

--
-- Data for Name: outschool_learner_pii; Type: TABLE DATA; Schema: public; Owner: jmh
--

COPY public.outschool_learner_pii (uid, email_address, personal_info, secret_token) FROM stdin;
8260c4a6-d9a7-40af-bdb1-7ada809faff6	8260c4a6-d9a7-40af-bdb1-7ada809faff6@obfuscated.outschool.com	vdxqi, ewfg ep uyeyqqsa nzesmm mmpe	\N
017c9eae-cac8-4661-bc45-520a99f9bee3	017c9eae-cac8-4661-bc45-520a99f9bee3@obfuscated.outschool.com	gy rxdgcc xxlx 0	\N
336d74cd-b905-4029-932d-6016bbd923c7	336d74cd-b905-4029-932d-6016bbd923c7@obfuscated.outschool.com	kycacnma dyad 9	\N
\.


--
-- PostgreSQL database dump complete
--

