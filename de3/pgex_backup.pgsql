--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- Name: box2d; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE box2d;


ALTER TYPE public.box2d OWNER TO postgres;

--
-- Name: box2df; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE box2df;


ALTER TYPE public.box2df OWNER TO postgres;

--
-- Name: box3d; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE box3d;


ALTER TYPE public.box3d OWNER TO postgres;

--
-- Name: geography; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE geography;


ALTER TYPE public.geography OWNER TO postgres;

--
-- Name: geometry; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE geometry;


ALTER TYPE public.geometry OWNER TO postgres;

--
-- Name: gidx; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE gidx;


ALTER TYPE public.gidx OWNER TO postgres;

--
-- Name: histogram; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE histogram AS (
	min double precision,
	max double precision,
	count bigint,
	percent double precision
);


ALTER TYPE public.histogram OWNER TO postgres;

--
-- Name: TYPE histogram; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE histogram IS 'postgis raster type: A composite type used as record output of the ST_Histogram and ST_ApproxHistogram functions.';


--
-- Name: pgis_abs; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE pgis_abs;


ALTER TYPE public.pgis_abs OWNER TO postgres;

--
-- Name: quantile; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE quantile AS (
	quantile double precision,
	value double precision
);


ALTER TYPE public.quantile OWNER TO postgres;

--
-- Name: raster; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE raster;


ALTER TYPE public.raster OWNER TO postgres;

--
-- Name: TYPE raster; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE raster IS 'postgis raster type: raster spatial data type.';


--
-- Name: reclassarg; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE reclassarg AS (
	nband integer,
	reclassexpr text,
	pixeltype text,
	nodataval double precision
);


ALTER TYPE public.reclassarg OWNER TO postgres;

--
-- Name: TYPE reclassarg; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE reclassarg IS 'postgis raster type: A composite type used as input into the ST_Reclass function defining the behavior of reclassification.';


--
-- Name: spheroid; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE spheroid;


ALTER TYPE public.spheroid OWNER TO postgres;

--
-- Name: summarystats; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE summarystats AS (
	count bigint,
	sum double precision,
	mean double precision,
	stddev double precision,
	min double precision,
	max double precision
);


ALTER TYPE public.summarystats OWNER TO postgres;

--
-- Name: TYPE summarystats; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TYPE summarystats IS 'postgis raster type: A composite type used as output of the ST_SummaryStats function.';


--
-- Name: valuecount; Type: TYPE; Schema: public; Owner: postgres
--

CREATE TYPE valuecount AS (
	value double precision,
	count integer,
	percent double precision
);


ALTER TYPE public.valuecount OWNER TO postgres;

--
-- Name: _add_overview_constraint(name, name, name, name, name, name, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_overview_constraint(ovschema name, ovtable name, ovcolumn name, refschema name, reftable name, refcolumn name, factor integer) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_overview_' || $3;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (_overview_constraint(' || quote_ident($3)
			|| ',' || $7
			|| ',' || quote_literal($4)
			|| ',' || quote_literal($5)
			|| ',' || quote_literal($6)
			|| '))';

		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_overview_constraint(ovschema name, ovtable name, ovcolumn name, refschema name, reftable name, refcolumn name, factor integer) OWNER TO postgres;

--
-- Name: _add_raster_constraint(name, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint(cn name, sql text) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $$
	BEGIN
		BEGIN
			EXECUTE sql;
		EXCEPTION
			WHEN duplicate_object THEN
				RAISE NOTICE 'The constraint "%" already exists.  To replace the existing constraint, delete the constraint and call ApplyRasterConstraints again', cn;
			WHEN OTHERS THEN
				RAISE NOTICE 'Unable to add constraint "%"', cn;
				RETURN FALSE;
		END;

		RETURN TRUE;
	END;
	$$;


ALTER FUNCTION public._add_raster_constraint(cn name, sql text) OWNER TO postgres;

--
-- Name: _add_raster_constraint_alignment(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_alignment(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr text;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_same_alignment_' || $3;

		sql := 'SELECT st_makeemptyraster(1, 1, upperleftx, upperlefty, scalex, scaley, skewx, skewy, srid) FROM st_metadata((SELECT '
			|| quote_ident($3)
			|| ' FROM ' || fqtn || ' LIMIT 1))';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the alignment of a sample raster';
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn ||
			' ADD CONSTRAINT ' || quote_ident(cn) ||
			' CHECK (st_samealignment(' || quote_ident($3) || ', ''' || attr || '''::raster))';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_alignment(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_blocksize(name, name, name, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr int;
	BEGIN
		IF lower($4) != 'width' AND lower($4) != 'height' THEN
			RAISE EXCEPTION 'axis must be either "width" or "height"';
			RETURN FALSE;
		END IF;

		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_' || $4 || '_' || $3;

		sql := 'SELECT st_' || $4 || '('
			|| quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the % of a sample raster', $4;
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (st_' || $4 || '('
			|| quote_ident($3)
			|| ') = ' || attr || ')';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) OWNER TO postgres;

--
-- Name: _add_raster_constraint_extent(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_extent(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr text;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_max_extent_' || $3;

		sql := 'SELECT st_ashexewkb(st_convexhull(st_collect(st_convexhull('
			|| quote_ident($3)
			|| ')))) FROM '
			|| fqtn;
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the extent of a sample raster';
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (st_coveredby(st_convexhull('
			|| quote_ident($3)
			|| '), ''' || attr || '''::geometry))';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_extent(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_nodata_values(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_nodata_values(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr double precision[];
		max int;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_nodata_values_' || $3;

		sql := 'SELECT _raster_constraint_nodata_values(' || quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the nodata values of a sample raster';
			RETURN FALSE;
		END;
		max := array_length(attr, 1);
		IF max < 1 OR max IS NULL THEN
			RAISE NOTICE 'Unable to get the nodata values of a sample raster';
			RETURN FALSE;
		END IF;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (_raster_constraint_nodata_values(' || quote_ident($3)
			|| ')::numeric(16,10)[] = ''{';
		FOR x in 1..max LOOP
			IF attr[x] IS NULL THEN
				sql := sql || 'NULL';
			ELSE
				sql := sql || attr[x];
			END IF;
			IF x < max THEN
				sql := sql || ',';
			END IF;
		END LOOP;
		sql := sql || '}''::numeric(16,10)[])';

		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_nodata_values(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_num_bands(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_num_bands(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr int;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_num_bands_' || $3;

		sql := 'SELECT st_numbands(' || quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the number of bands of a sample raster';
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (st_numbands(' || quote_ident($3)
			|| ') = ' || attr
			|| ')';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_num_bands(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_out_db(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_out_db(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr boolean[];
		max int;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_out_db_' || $3;

		sql := 'SELECT _raster_constraint_out_db(' || quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the out-of-database bands of a sample raster';
			RETURN FALSE;
		END;
		max := array_length(attr, 1);
		IF max < 1 OR max IS NULL THEN
			RAISE NOTICE 'Unable to get the out-of-database bands of a sample raster';
			RETURN FALSE;
		END IF;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (_raster_constraint_out_db(' || quote_ident($3)
			|| ') = ''{';
		FOR x in 1..max LOOP
			IF attr[x] IS FALSE THEN
				sql := sql || 'FALSE';
			ELSE
				sql := sql || 'TRUE';
			END IF;
			IF x < max THEN
				sql := sql || ',';
			END IF;
		END LOOP;
		sql := sql || '}''::boolean[])';

		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_out_db(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_pixel_types(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_pixel_types(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr text[];
		max int;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_pixel_types_' || $3;

		sql := 'SELECT _raster_constraint_pixel_types(' || quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the pixel types of a sample raster';
			RETURN FALSE;
		END;
		max := array_length(attr, 1);
		IF max < 1 OR max IS NULL THEN
			RAISE NOTICE 'Unable to get the pixel types of a sample raster';
			RETURN FALSE;
		END IF;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (_raster_constraint_pixel_types(' || quote_ident($3)
			|| ') = ''{';
		FOR x in 1..max LOOP
			sql := sql || '"' || attr[x] || '"';
			IF x < max THEN
				sql := sql || ',';
			END IF;
		END LOOP;
		sql := sql || '}''::text[])';

		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_pixel_types(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_regular_blocking(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_regular_blocking(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
	BEGIN

		RAISE INFO 'The regular_blocking constraint is just a flag indicating that the column "%" is regularly blocked.  It is up to the end-user to ensure that the column is truely regularly blocked.', quote_ident($3);

		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_regular_blocking_' || $3;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (TRUE)';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_regular_blocking(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _add_raster_constraint_scale(name, name, name, character); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_scale(rastschema name, rasttable name, rastcolumn name, axis character) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr double precision;
	BEGIN
		IF lower($4) != 'x' AND lower($4) != 'y' THEN
			RAISE EXCEPTION 'axis must be either "x" or "y"';
			RETURN FALSE;
		END IF;

		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_scale' || $4 || '_' || $3;

		sql := 'SELECT st_scale' || $4 || '('
			|| quote_ident($3)
			|| ') FROM '
			|| fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the %-scale of a sample raster', upper($4);
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (st_scale' || $4 || '('
			|| quote_ident($3)
			|| ')::numeric(16,10) = (' || attr || ')::numeric(16,10))';
		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_scale(rastschema name, rasttable name, rastcolumn name, axis character) OWNER TO postgres;

--
-- Name: _add_raster_constraint_srid(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _add_raster_constraint_srid(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
		cn name;
		sql text;
		attr int;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		cn := 'enforce_srid_' || $3;

		sql := 'SELECT st_srid('
			|| quote_ident($3)
			|| ') FROM ' || fqtn
			|| ' LIMIT 1';
		BEGIN
			EXECUTE sql INTO attr;
		EXCEPTION WHEN OTHERS THEN
			RAISE NOTICE 'Unable to get the SRID of a sample raster';
			RETURN FALSE;
		END;

		sql := 'ALTER TABLE ' || fqtn
			|| ' ADD CONSTRAINT ' || quote_ident(cn)
			|| ' CHECK (st_srid('
			|| quote_ident($3)
			|| ') = ' || attr || ')';

		RETURN _add_raster_constraint(cn, sql);
	END;
	$_$;


ALTER FUNCTION public._add_raster_constraint_srid(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_overview_constraint(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_overview_constraint(ovschema name, ovtable name, ovcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_overview_' || $3) $_$;


ALTER FUNCTION public._drop_overview_constraint(ovschema name, ovtable name, ovcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint(rastschema name, rasttable name, cn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		fqtn text;
	BEGIN
		fqtn := '';
		IF length($1) > 0 THEN
			fqtn := quote_ident($1) || '.';
		END IF;
		fqtn := fqtn || quote_ident($2);

		BEGIN
			EXECUTE 'ALTER TABLE '
				|| fqtn
				|| ' DROP CONSTRAINT '
				|| quote_ident(cn);
			RETURN TRUE;
		EXCEPTION
			WHEN undefined_object THEN
				RAISE NOTICE 'The constraint "%" does not exist.  Skipping', cn;
			WHEN OTHERS THEN
				RAISE NOTICE 'Unable to drop constraint "%"', cn;
				RETURN FALSE;
		END;

		RETURN TRUE;
	END;
	$_$;


ALTER FUNCTION public._drop_raster_constraint(rastschema name, rasttable name, cn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_alignment(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_alignment(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_same_alignment_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_alignment(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_blocksize(name, name, name, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	BEGIN
		IF lower($4) != 'width' AND lower($4) != 'height' THEN
			RAISE EXCEPTION 'axis must be either "width" or "height"';
			RETURN FALSE;
		END IF;

		RETURN _drop_raster_constraint($1, $2, 'enforce_' || $4 || '_' || $3);
	END;
	$_$;


ALTER FUNCTION public._drop_raster_constraint_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_extent(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_extent(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_max_extent_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_extent(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_nodata_values(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_nodata_values(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_nodata_values_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_nodata_values(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_num_bands(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_num_bands(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_num_bands_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_num_bands(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_out_db(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_out_db(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_out_db_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_out_db(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_pixel_types(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_pixel_types(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_pixel_types_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_pixel_types(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_regular_blocking(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_regular_blocking(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_regular_blocking_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_regular_blocking(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_scale(name, name, name, character); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_scale(rastschema name, rasttable name, rastcolumn name, axis character) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	BEGIN
		IF lower($4) != 'x' AND lower($4) != 'y' THEN
			RAISE EXCEPTION 'axis must be either "x" or "y"';
			RETURN FALSE;
		END IF;

		RETURN _drop_raster_constraint($1, $2, 'enforce_scale' || $4 || '_' || $3);
	END;
	$_$;


ALTER FUNCTION public._drop_raster_constraint_scale(rastschema name, rasttable name, rastcolumn name, axis character) OWNER TO postgres;

--
-- Name: _drop_raster_constraint_srid(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _drop_raster_constraint_srid(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT _drop_raster_constraint($1, $2, 'enforce_srid_' || $3) $_$;


ALTER FUNCTION public._drop_raster_constraint_srid(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _overview_constraint_info(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _overview_constraint_info(ovschema name, ovtable name, ovcolumn name, OUT refschema name, OUT reftable name, OUT refcolumn name, OUT factor integer) RETURNS record
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		split_part(split_part(s.consrc, '''::name', 1), '''', 2)::name,
		split_part(split_part(s.consrc, '''::name', 2), '''', 2)::name,
		split_part(split_part(s.consrc, '''::name', 3), '''', 2)::name,
		trim(both from split_part(s.consrc, ',', 2))::integer
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%_overview_constraint(%'
	$_$;


ALTER FUNCTION public._overview_constraint_info(ovschema name, ovtable name, ovcolumn name, OUT refschema name, OUT reftable name, OUT refcolumn name, OUT factor integer) OWNER TO postgres;

--
-- Name: _raster_constraint_info_alignment(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_alignment(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		TRUE
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%st_samealignment(%';
	$_$;


ALTER FUNCTION public._raster_constraint_info_alignment(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_blocksize(name, name, name, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', '')::integer
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%st_' || $4 || '(% = %';
	$_$;


ALTER FUNCTION public._raster_constraint_info_blocksize(rastschema name, rasttable name, rastcolumn name, axis text) OWNER TO postgres;

--
-- Name: _raster_constraint_info_nodata_values(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_nodata_values(rastschema name, rasttable name, rastcolumn name) RETURNS double precision[]
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		trim(both '''' from split_part(replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', ''), '::', 1))::double precision[]
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%_raster_constraint_nodata_values(%';
	$_$;


ALTER FUNCTION public._raster_constraint_info_nodata_values(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_num_bands(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_num_bands(rastschema name, rasttable name, rastcolumn name) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', '')::integer
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%st_numbands(%';
	$_$;


ALTER FUNCTION public._raster_constraint_info_num_bands(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_out_db(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_out_db(rastschema name, rasttable name, rastcolumn name) RETURNS boolean[]
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		trim(both '''' from split_part(replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', ''), '::', 1))::boolean[]
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%_raster_constraint_out_db(%';
	$_$;


ALTER FUNCTION public._raster_constraint_info_out_db(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_pixel_types(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_pixel_types(rastschema name, rasttable name, rastcolumn name) RETURNS text[]
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		trim(both '''' from split_part(replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', ''), '::', 1))::text[]
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%_raster_constraint_pixel_types(%';
	$_$;


ALTER FUNCTION public._raster_constraint_info_pixel_types(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_regular_blocking(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_regular_blocking(rastschema name, rasttable name, rastcolumn name) RETURNS boolean
    LANGUAGE plpgsql STABLE STRICT
    AS $_$
	DECLARE
		cn text;
		sql text;
		rtn boolean;
	BEGIN
		cn := 'enforce_regular_blocking_' || $3;

		sql := 'SELECT TRUE FROM pg_class c, pg_namespace n, pg_constraint s'
			|| ' WHERE n.nspname = ' || quote_literal($1)
			|| ' AND c.relname = ' || quote_literal($2)
			|| ' AND s.connamespace = n.oid AND s.conrelid = c.oid'
			|| ' AND s.conname = ' || quote_literal(cn);
		EXECUTE sql INTO rtn;
		RETURN rtn;
	END;
	$_$;


ALTER FUNCTION public._raster_constraint_info_regular_blocking(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _raster_constraint_info_scale(name, name, name, character); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_scale(rastschema name, rasttable name, rastcolumn name, axis character) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		replace(replace(split_part(split_part(s.consrc, ' = ', 2), '::', 1), ')', ''), '(', '')::double precision
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%st_scale' || $4 || '(% = %';
	$_$;


ALTER FUNCTION public._raster_constraint_info_scale(rastschema name, rasttable name, rastcolumn name, axis character) OWNER TO postgres;

--
-- Name: _raster_constraint_info_srid(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _raster_constraint_info_srid(rastschema name, rasttable name, rastcolumn name) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$
	SELECT
		replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', '')::integer
	FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
	WHERE n.nspname = $1
		AND c.relname = $2
		AND a.attname = $3
		AND a.attrelid = c.oid
		AND s.connamespace = n.oid
		AND s.conrelid = c.oid
		AND a.attnum = ANY (s.conkey)
		AND s.consrc LIKE '%st_srid(% = %';
	$_$;


ALTER FUNCTION public._raster_constraint_info_srid(rastschema name, rasttable name, rastcolumn name) OWNER TO postgres;

--
-- Name: _st_aspect4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _st_aspect4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        pwidth float;
        pheight float;
        dz_dx float;
        dz_dy float;
        aspect float;
    BEGIN
        pwidth := args[1]::float;
        pheight := args[2]::float;
        dz_dx := ((matrix[3][1] + 2.0 * matrix[3][2] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[1][2] + matrix[1][3])) / (8.0 * pwidth);
        dz_dy := ((matrix[1][3] + 2.0 * matrix[2][3] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[2][1] + matrix[3][1])) / (8.0 * pheight);
        IF abs(dz_dx) = 0::float AND abs(dz_dy) = 0::float THEN
            RETURN -1;
        END IF;

        aspect := atan2(dz_dy, -dz_dx);
        IF aspect > (pi() / 2.0) THEN
            RETURN (5.0 * pi() / 2.0) - aspect;
        ELSE
            RETURN (pi() / 2.0) - aspect;
        END IF;
    END;
    $$;


ALTER FUNCTION public._st_aspect4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: _st_count(text, text, integer, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _st_count(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, sample_percent double precision DEFAULT 1) RETURNS bigint
    LANGUAGE plpgsql STABLE STRICT
    AS $_$
	DECLARE
		curs refcursor;

		ctable text;
		ccolumn text;
		rast raster;
		stats summarystats;

		rtn bigint;
		tmp bigint;
	BEGIN
		-- nband
		IF nband < 1 THEN
			RAISE WARNING 'Invalid band index (must use 1-based). Returning NULL';
			RETURN NULL;
		END IF;

		-- sample percent
		IF sample_percent < 0 OR sample_percent > 1 THEN
			RAISE WARNING 'Invalid sample percentage (must be between 0 and 1). Returning NULL';
			RETURN NULL;
		END IF;

		-- exclude_nodata_value IS TRUE
		IF exclude_nodata_value IS TRUE THEN
			SELECT count INTO rtn FROM _st_summarystats($1, $2, $3, $4, $5);
			RETURN rtn;
		END IF;

		-- clean rastertable and rastercolumn
		ctable := quote_ident(rastertable);
		ccolumn := quote_ident(rastercolumn);

		BEGIN
			OPEN curs FOR EXECUTE 'SELECT '
					|| ccolumn
					|| ' FROM '
					|| ctable
					|| ' WHERE '
					|| ccolumn
					|| ' IS NOT NULL';
		EXCEPTION
			WHEN OTHERS THEN
				RAISE WARNING 'Invalid table or column name. Returning NULL';
				RETURN NULL;
		END;

		rtn := 0;
		LOOP
			FETCH curs INTO rast;
			EXIT WHEN NOT FOUND;

			SELECT (width * height) INTO tmp FROM ST_Metadata(rast);
			rtn := rtn + tmp;
		END LOOP;

		CLOSE curs;

		RETURN rtn;
	END;
	$_$;


ALTER FUNCTION public._st_count(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision) OWNER TO postgres;

--
-- Name: _st_hillshade4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _st_hillshade4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        pwidth float;
        pheight float;
        dz_dx float;
        dz_dy float;
        zenith float;
        azimuth float;
        slope float;
        aspect float;
        max_bright float;
        elevation_scale float;
    BEGIN
        pwidth := args[1]::float;
        pheight := args[2]::float;
        azimuth := (5.0 * pi() / 2.0) - args[3]::float;
        zenith := (pi() / 2.0) - args[4]::float;
        dz_dx := ((matrix[3][1] + 2.0 * matrix[3][2] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[1][2] + matrix[1][3])) / (8.0 * pwidth);
        dz_dy := ((matrix[1][3] + 2.0 * matrix[2][3] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[2][1] + matrix[3][1])) / (8.0 * pheight);
        elevation_scale := args[6]::float;
        slope := atan(sqrt(elevation_scale * pow(dz_dx, 2.0) + pow(dz_dy, 2.0)));
        -- handle special case of 0, 0
        IF abs(dz_dy) = 0::float AND abs(dz_dy) = 0::float THEN
            -- set to pi as that is the expected PostgreSQL answer in Linux
            aspect := pi();
        ELSE
            aspect := atan2(dz_dy, -dz_dx);
        END IF;
        max_bright := args[5]::float;

        IF aspect < 0 THEN
            aspect := aspect + (2.0 * pi());
        END IF;

        RETURN max_bright * ( (cos(zenith)*cos(slope)) + (sin(zenith)*sin(slope)*cos(azimuth - aspect)) );
    END;
    $$;


ALTER FUNCTION public._st_hillshade4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: _st_slope4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION _st_slope4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        pwidth float;
        pheight float;
        dz_dx float;
        dz_dy float;
    BEGIN
        pwidth := args[1]::float;
        pheight := args[2]::float;
        dz_dx := ((matrix[3][1] + 2.0 * matrix[3][2] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[1][2] + matrix[1][3])) / (8.0 * pwidth);
        dz_dy := ((matrix[1][3] + 2.0 * matrix[2][3] + matrix[3][3]) - (matrix[1][1] + 2.0 * matrix[2][1] + matrix[3][1])) / (8.0 * pheight);
        RETURN atan(sqrt(pow(dz_dx, 2.0) + pow(dz_dy, 2.0)));
    END;
    $$;


ALTER FUNCTION public._st_slope4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: addauth(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addauth(text) RETURNS boolean
    LANGUAGE plpgsql
    AS $_$ 
DECLARE
	lockid alias for $1;
	okay boolean;
	myrec record;
BEGIN
	-- check to see if table exists
	--  if not, CREATE TEMP TABLE mylock (transid xid, lockcode text)
	okay := 'f';
	FOR myrec IN SELECT * FROM pg_class WHERE relname = 'temp_lock_have_table' LOOP
		okay := 't';
	END LOOP; 
	IF (okay <> 't') THEN 
		CREATE TEMP TABLE temp_lock_have_table (transid xid, lockcode text);
			-- this will only work from pgsql7.4 up
			-- ON COMMIT DELETE ROWS;
	END IF;

	--  INSERT INTO mylock VALUES ( $1)
--	EXECUTE 'INSERT INTO temp_lock_have_table VALUES ( '||
--		quote_literal(getTransactionID()) || ',' ||
--		quote_literal(lockid) ||')';

	INSERT INTO temp_lock_have_table VALUES (getTransactionID(), lockid);

	RETURN true::boolean;
END;
$_$;


ALTER FUNCTION public.addauth(text) OWNER TO postgres;

--
-- Name: addgeometrycolumn(character varying, character varying, integer, character varying, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addgeometrycolumn(table_name character varying, column_name character varying, new_srid integer, new_type character varying, new_dim integer, use_typmod boolean DEFAULT true) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $_$
DECLARE
	ret  text;
BEGIN
	SELECT AddGeometryColumn('','',$1,$2,$3,$4,$5, $6) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.addgeometrycolumn(table_name character varying, column_name character varying, new_srid integer, new_type character varying, new_dim integer, use_typmod boolean) OWNER TO postgres;

--
-- Name: addgeometrycolumn(character varying, character varying, character varying, integer, character varying, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addgeometrycolumn(schema_name character varying, table_name character varying, column_name character varying, new_srid integer, new_type character varying, new_dim integer, use_typmod boolean DEFAULT true) RETURNS text
    LANGUAGE plpgsql STABLE STRICT
    AS $_$
DECLARE
	ret  text;
BEGIN
	SELECT AddGeometryColumn('',$1,$2,$3,$4,$5,$6,$7) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.addgeometrycolumn(schema_name character varying, table_name character varying, column_name character varying, new_srid integer, new_type character varying, new_dim integer, use_typmod boolean) OWNER TO postgres;

--
-- Name: addgeometrycolumn(character varying, character varying, character varying, character varying, integer, character varying, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addgeometrycolumn(catalog_name character varying, schema_name character varying, table_name character varying, column_name character varying, new_srid_in integer, new_type character varying, new_dim integer, use_typmod boolean DEFAULT true) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $$
DECLARE
	rec RECORD;
	sr varchar;
	real_schema name;
	sql text;
	new_srid integer;

BEGIN

	-- Verify geometry type
	IF (postgis_type_name(new_type,new_dim) IS NULL )
	THEN
		RAISE EXCEPTION 'Invalid type name "%(%)" - valid ones are:
	POINT, MULTIPOINT,
	LINESTRING, MULTILINESTRING,
	POLYGON, MULTIPOLYGON,
	CIRCULARSTRING, COMPOUNDCURVE, MULTICURVE,
	CURVEPOLYGON, MULTISURFACE,
	GEOMETRY, GEOMETRYCOLLECTION,
	POINTM, MULTIPOINTM,
	LINESTRINGM, MULTILINESTRINGM,
	POLYGONM, MULTIPOLYGONM,
	CIRCULARSTRINGM, COMPOUNDCURVEM, MULTICURVEM
	CURVEPOLYGONM, MULTISURFACEM, TRIANGLE, TRIANGLEM,
	POLYHEDRALSURFACE, POLYHEDRALSURFACEM, TIN, TINM
	or GEOMETRYCOLLECTIONM', new_type, new_dim;
		RETURN 'fail';
	END IF;


	-- Verify dimension
	IF ( (new_dim >4) OR (new_dim <2) ) THEN
		RAISE EXCEPTION 'invalid dimension';
		RETURN 'fail';
	END IF;

	IF ( (new_type LIKE '%M') AND (new_dim!=3) ) THEN
		RAISE EXCEPTION 'TypeM needs 3 dimensions';
		RETURN 'fail';
	END IF;


	-- Verify SRID
	IF ( new_srid_in > 0 ) THEN
		IF new_srid_in > 998999 THEN
			RAISE EXCEPTION 'AddGeometryColumn() - SRID must be <= %', 998999;
		END IF;
		new_srid := new_srid_in;
		SELECT SRID INTO sr FROM spatial_ref_sys WHERE SRID = new_srid;
		IF NOT FOUND THEN
			RAISE EXCEPTION 'AddGeometryColumn() - invalid SRID';
			RETURN 'fail';
		END IF;
	ELSE
		new_srid := ST_SRID('POINT EMPTY'::geometry);
		IF ( new_srid_in != new_srid ) THEN
			RAISE NOTICE 'SRID value % converted to the officially unknown SRID value %', new_srid_in, new_srid;
		END IF;
	END IF;


	-- Verify schema
	IF ( schema_name IS NOT NULL AND schema_name != '' ) THEN
		sql := 'SELECT nspname FROM pg_namespace ' ||
			'WHERE text(nspname) = ' || quote_literal(schema_name) ||
			'LIMIT 1';
		RAISE DEBUG '%', sql;
		EXECUTE sql INTO real_schema;

		IF ( real_schema IS NULL ) THEN
			RAISE EXCEPTION 'Schema % is not a valid schemaname', quote_literal(schema_name);
			RETURN 'fail';
		END IF;
	END IF;

	IF ( real_schema IS NULL ) THEN
		RAISE DEBUG 'Detecting schema';
		sql := 'SELECT n.nspname AS schemaname ' ||
			'FROM pg_catalog.pg_class c ' ||
			  'JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ' ||
			'WHERE c.relkind = ' || quote_literal('r') ||
			' AND n.nspname NOT IN (' || quote_literal('pg_catalog') || ', ' || quote_literal('pg_toast') || ')' ||
			' AND pg_catalog.pg_table_is_visible(c.oid)' ||
			' AND c.relname = ' || quote_literal(table_name);
		RAISE DEBUG '%', sql;
		EXECUTE sql INTO real_schema;

		IF ( real_schema IS NULL ) THEN
			RAISE EXCEPTION 'Table % does not occur in the search_path', quote_literal(table_name);
			RETURN 'fail';
		END IF;
	END IF;


	-- Add geometry column to table
	IF use_typmod THEN
	     sql := 'ALTER TABLE ' ||
            quote_ident(real_schema) || '.' || quote_ident(table_name)
            || ' ADD COLUMN ' || quote_ident(column_name) ||
            ' geometry(' || postgis_type_name(new_type, new_dim) || ', ' || new_srid::text || ')';
        RAISE DEBUG '%', sql;
	ELSE
        sql := 'ALTER TABLE ' ||
            quote_ident(real_schema) || '.' || quote_ident(table_name)
            || ' ADD COLUMN ' || quote_ident(column_name) ||
            ' geometry ';
        RAISE DEBUG '%', sql;
    END IF;
	EXECUTE sql;

	IF NOT use_typmod THEN
        -- Add table CHECKs
        sql := 'ALTER TABLE ' ||
            quote_ident(real_schema) || '.' || quote_ident(table_name)
            || ' ADD CONSTRAINT '
            || quote_ident('enforce_srid_' || column_name)
            || ' CHECK (st_srid(' || quote_ident(column_name) ||
            ') = ' || new_srid::text || ')' ;
        RAISE DEBUG '%', sql;
        EXECUTE sql;
    
        sql := 'ALTER TABLE ' ||
            quote_ident(real_schema) || '.' || quote_ident(table_name)
            || ' ADD CONSTRAINT '
            || quote_ident('enforce_dims_' || column_name)
            || ' CHECK (st_ndims(' || quote_ident(column_name) ||
            ') = ' || new_dim::text || ')' ;
        RAISE DEBUG '%', sql;
        EXECUTE sql;
    
        IF ( NOT (new_type = 'GEOMETRY')) THEN
            sql := 'ALTER TABLE ' ||
                quote_ident(real_schema) || '.' || quote_ident(table_name) || ' ADD CONSTRAINT ' ||
                quote_ident('enforce_geotype_' || column_name) ||
                ' CHECK (GeometryType(' ||
                quote_ident(column_name) || ')=' ||
                quote_literal(new_type) || ' OR (' ||
                quote_ident(column_name) || ') is null)';
            RAISE DEBUG '%', sql;
            EXECUTE sql;
        END IF;
    END IF;

	RETURN
		real_schema || '.' ||
		table_name || '.' || column_name ||
		' SRID:' || new_srid::text ||
		' TYPE:' || new_type ||
		' DIMS:' || new_dim::text || ' ';
END;
$$;


ALTER FUNCTION public.addgeometrycolumn(catalog_name character varying, schema_name character varying, table_name character varying, column_name character varying, new_srid_in integer, new_type character varying, new_dim integer, use_typmod boolean) OWNER TO postgres;

--
-- Name: addoverviewconstraints(name, name, name, name, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addoverviewconstraints(ovtable name, ovcolumn name, reftable name, refcolumn name, ovfactor integer) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT AddOverviewConstraints('', $1, $2, '', $3, $4, $5) $_$;


ALTER FUNCTION public.addoverviewconstraints(ovtable name, ovcolumn name, reftable name, refcolumn name, ovfactor integer) OWNER TO postgres;

--
-- Name: addoverviewconstraints(name, name, name, name, name, name, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addoverviewconstraints(ovschema name, ovtable name, ovcolumn name, refschema name, reftable name, refcolumn name, ovfactor integer) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		x int;
		s name;
		t name;
		oschema name;
		rschema name;
		sql text;
		rtn boolean;
	BEGIN
		FOR x IN 1..2 LOOP
			s := '';

			IF x = 1 THEN
				s := $1;
				t := $2;
			ELSE
				s := $4;
				t := $5;
			END IF;

			-- validate user-provided schema
			IF length(s) > 0 THEN
				sql := 'SELECT nspname FROM pg_namespace '
					|| 'WHERE nspname = ' || quote_literal(s)
					|| 'LIMIT 1';
				EXECUTE sql INTO s;

				IF s IS NULL THEN
					RAISE EXCEPTION 'The value % is not a valid schema', quote_literal(s);
					RETURN FALSE;
				END IF;
			END IF;

			-- no schema, determine what it could be using the table
			IF length(s) < 1 THEN
				sql := 'SELECT n.nspname AS schemaname '
					|| 'FROM pg_catalog.pg_class c '
					|| 'JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
					|| 'WHERE c.relkind = ' || quote_literal('r')
					|| ' AND n.nspname NOT IN (' || quote_literal('pg_catalog')
					|| ', ' || quote_literal('pg_toast')
					|| ') AND pg_catalog.pg_table_is_visible(c.oid)'
					|| ' AND c.relname = ' || quote_literal(t);
				EXECUTE sql INTO s;

				IF s IS NULL THEN
					RAISE EXCEPTION 'The table % does not occur in the search_path', quote_literal(t);
					RETURN FALSE;
				END IF;
			END IF;

			IF x = 1 THEN
				oschema := s;
			ELSE
				rschema := s;
			END IF;
		END LOOP;

		-- reference raster
		rtn := _add_overview_constraint(oschema, $2, $3, rschema, $5, $6, $7);
		IF rtn IS FALSE THEN
			RAISE EXCEPTION 'Unable to add the overview constraint.  Is the schema name, table name or column name incorrect?';
			RETURN FALSE;
		END IF;

		RETURN TRUE;
	END;
	$_$;


ALTER FUNCTION public.addoverviewconstraints(ovschema name, ovtable name, ovcolumn name, refschema name, reftable name, refcolumn name, ovfactor integer) OWNER TO postgres;

--
-- Name: addrasterconstraints(name, name, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addrasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT AddRasterConstraints('', $1, $2, VARIADIC $3) $_$;


ALTER FUNCTION public.addrasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]) OWNER TO postgres;

--
-- Name: FUNCTION addrasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION addrasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]) IS 'args: rasttable, rastcolumn, VARIADIC constraints - Adds raster constraints to a loaded raster table for a specific column that constrains spatial ref, scaling, blocksize, alignment, bands, band type and a flag to denote if raster column is regularly blocked. The table must be loaded with data for the constraints to be inferred. Returns true of the constraint setting was accomplished and if issues a notice.';


--
-- Name: addrasterconstraints(name, name, name, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		max int;
		cnt int;
		sql text;
		schema name;
		x int;
		kw text;
		rtn boolean;
	BEGIN
		cnt := 0;
		max := array_length(constraints, 1);
		IF max < 1 THEN
			RAISE NOTICE 'No constraints indicated to be added.  Doing nothing';
			RETURN TRUE;
		END IF;

		-- validate schema
		schema := NULL;
		IF length($1) > 0 THEN
			sql := 'SELECT nspname FROM pg_namespace '
				|| 'WHERE nspname = ' || quote_literal($1)
				|| 'LIMIT 1';
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The value provided for schema is invalid';
				RETURN FALSE;
			END IF;
		END IF;

		IF schema IS NULL THEN
			sql := 'SELECT n.nspname AS schemaname '
				|| 'FROM pg_catalog.pg_class c '
				|| 'JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
				|| 'WHERE c.relkind = ' || quote_literal('r')
				|| ' AND n.nspname NOT IN (' || quote_literal('pg_catalog')
				|| ', ' || quote_literal('pg_toast')
				|| ') AND pg_catalog.pg_table_is_visible(c.oid)'
				|| ' AND c.relname = ' || quote_literal($2);
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The table % does not occur in the search_path', quote_literal($2);
				RETURN FALSE;
			END IF;
		END IF;

		<<kwloop>>
		FOR x in 1..max LOOP
			kw := trim(both from lower(constraints[x]));

			BEGIN
				CASE
					WHEN kw = 'srid' THEN
						RAISE NOTICE 'Adding SRID constraint';
						rtn := _add_raster_constraint_srid(schema, $2, $3);
					WHEN kw IN ('scale_x', 'scalex') THEN
						RAISE NOTICE 'Adding scale-X constraint';
						rtn := _add_raster_constraint_scale(schema, $2, $3, 'x');
					WHEN kw IN ('scale_y', 'scaley') THEN
						RAISE NOTICE 'Adding scale-Y constraint';
						rtn := _add_raster_constraint_scale(schema, $2, $3, 'y');
					WHEN kw = 'scale' THEN
						RAISE NOTICE 'Adding scale-X constraint';
						rtn := _add_raster_constraint_scale(schema, $2, $3, 'x');
						RAISE NOTICE 'Adding scale-Y constraint';
						rtn := _add_raster_constraint_scale(schema, $2, $3, 'y');
					WHEN kw IN ('blocksize_x', 'blocksizex', 'width') THEN
						RAISE NOTICE 'Adding blocksize-X constraint';
						rtn := _add_raster_constraint_blocksize(schema, $2, $3, 'width');
					WHEN kw IN ('blocksize_y', 'blocksizey', 'height') THEN
						RAISE NOTICE 'Adding blocksize-Y constraint';
						rtn := _add_raster_constraint_blocksize(schema, $2, $3, 'height');
					WHEN kw = 'blocksize' THEN
						RAISE NOTICE 'Adding blocksize-X constraint';
						rtn := _add_raster_constraint_blocksize(schema, $2, $3, 'width');
						RAISE NOTICE 'Adding blocksize-Y constraint';
						rtn := _add_raster_constraint_blocksize(schema, $2, $3, 'height');
					WHEN kw IN ('same_alignment', 'samealignment', 'alignment') THEN
						RAISE NOTICE 'Adding alignment constraint';
						rtn := _add_raster_constraint_alignment(schema, $2, $3);
					WHEN kw IN ('regular_blocking', 'regularblocking') THEN
						RAISE NOTICE 'Adding regular blocking constraint';
						rtn := _add_raster_constraint_regular_blocking(schema, $2, $3);
					WHEN kw IN ('num_bands', 'numbands') THEN
						RAISE NOTICE 'Adding number of bands constraint';
						rtn := _add_raster_constraint_num_bands(schema, $2, $3);
					WHEN kw IN ('pixel_types', 'pixeltypes') THEN
						RAISE NOTICE 'Adding pixel type constraint';
						rtn := _add_raster_constraint_pixel_types(schema, $2, $3);
					WHEN kw IN ('nodata_values', 'nodatavalues', 'nodata') THEN
						RAISE NOTICE 'Adding nodata value constraint';
						rtn := _add_raster_constraint_nodata_values(schema, $2, $3);
					WHEN kw IN ('out_db', 'outdb') THEN
						RAISE NOTICE 'Adding out-of-database constraint';
						rtn := _add_raster_constraint_out_db(schema, $2, $3);
					WHEN kw = 'extent' THEN
						RAISE NOTICE 'Adding maximum extent constraint';
						rtn := _add_raster_constraint_extent(schema, $2, $3);
					ELSE
						RAISE NOTICE 'Unknown constraint: %.  Skipping', quote_literal(constraints[x]);
						CONTINUE kwloop;
				END CASE;
			END;

			IF rtn IS FALSE THEN
				cnt := cnt + 1;
				RAISE WARNING 'Unable to add constraint: %.  Skipping', quote_literal(constraints[x]);
			END IF;

		END LOOP kwloop;

		IF cnt = max THEN
			RAISE EXCEPTION 'None of the constraints specified could be added.  Is the schema name, table name or column name incorrect?';
			RETURN FALSE;
		END IF;

		RETURN TRUE;
	END;
	$_$;


ALTER FUNCTION public.addrasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) OWNER TO postgres;

--
-- Name: FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) IS 'args: rastschema, rasttable, rastcolumn, VARIADIC constraints - Adds raster constraints to a loaded raster table for a specific column that constrains spatial ref, scaling, blocksize, alignment, bands, band type and a flag to denote if raster column is regularly blocked. The table must be loaded with data for the constraints to be inferred. Returns true of the constraint setting was accomplished and if issues a notice.';


--
-- Name: addrasterconstraints(name, name, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addrasterconstraints(rasttable name, rastcolumn name, srid boolean DEFAULT true, scale_x boolean DEFAULT true, scale_y boolean DEFAULT true, blocksize_x boolean DEFAULT true, blocksize_y boolean DEFAULT true, same_alignment boolean DEFAULT true, regular_blocking boolean DEFAULT false, num_bands boolean DEFAULT true, pixel_types boolean DEFAULT true, nodata_values boolean DEFAULT true, out_db boolean DEFAULT true, extent boolean DEFAULT true) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT AddRasterConstraints('', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) $_$;


ALTER FUNCTION public.addrasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) OWNER TO postgres;

--
-- Name: FUNCTION addrasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION addrasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) IS 'args: rasttable, rastcolumn, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands=true, pixel_types=true, nodata_values=true, out_db=true, extent=true - Adds raster constraints to a loaded raster table for a specific column that constrains spatial ref, scaling, blocksize, alignment, bands, band type and a flag to denote if raster column is regularly blocked. The table must be loaded with data for the constraints to be inferred. Returns true of the constraint setting was accomplished and if issues a notice.';


--
-- Name: addrasterconstraints(name, name, name, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean DEFAULT true, scale_x boolean DEFAULT true, scale_y boolean DEFAULT true, blocksize_x boolean DEFAULT true, blocksize_y boolean DEFAULT true, same_alignment boolean DEFAULT true, regular_blocking boolean DEFAULT false, num_bands boolean DEFAULT true, pixel_types boolean DEFAULT true, nodata_values boolean DEFAULT true, out_db boolean DEFAULT true, extent boolean DEFAULT true) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		constraints text[];
	BEGIN
		IF srid IS TRUE THEN
			constraints := constraints || 'srid'::text;
		END IF;

		IF scale_x IS TRUE THEN
			constraints := constraints || 'scale_x'::text;
		END IF;

		IF scale_y IS TRUE THEN
			constraints := constraints || 'scale_y'::text;
		END IF;

		IF blocksize_x IS TRUE THEN
			constraints := constraints || 'blocksize_x'::text;
		END IF;

		IF blocksize_y IS TRUE THEN
			constraints := constraints || 'blocksize_y'::text;
		END IF;

		IF same_alignment IS TRUE THEN
			constraints := constraints || 'same_alignment'::text;
		END IF;

		IF regular_blocking IS TRUE THEN
			constraints := constraints || 'regular_blocking'::text;
		END IF;

		IF num_bands IS TRUE THEN
			constraints := constraints || 'num_bands'::text;
		END IF;

		IF pixel_types IS TRUE THEN
			constraints := constraints || 'pixel_types'::text;
		END IF;

		IF nodata_values IS TRUE THEN
			constraints := constraints || 'nodata_values'::text;
		END IF;

		IF out_db IS TRUE THEN
			constraints := constraints || 'out_db'::text;
		END IF;

		IF extent IS TRUE THEN
			constraints := constraints || 'extent'::text;
		END IF;

		RETURN AddRasterConstraints($1, $2, $3, VARIADIC constraints);
	END;
	$_$;


ALTER FUNCTION public.addrasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) OWNER TO postgres;

--
-- Name: FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION addrasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) IS 'args: rastschema, rasttable, rastcolumn, srid=true, scale_x=true, scale_y=true, blocksize_x=true, blocksize_y=true, same_alignment=true, regular_blocking=true, num_bands=true, pixel_types=true, nodata_values=true, out_db=true, extent=true - Adds raster constraints to a loaded raster table for a specific column that constrains spatial ref, scaling, blocksize, alignment, bands, band type and a flag to denote if raster column is regularly blocked. The table must be loaded with data for the constraints to be inferred. Returns true of the constraint setting was accomplished and if issues a notice.';


--
-- Name: checkauth(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION checkauth(text, text) RETURNS integer
    LANGUAGE sql
    AS $_$ SELECT CheckAuth('', $1, $2) $_$;


ALTER FUNCTION public.checkauth(text, text) OWNER TO postgres;

--
-- Name: checkauth(text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION checkauth(text, text, text) RETURNS integer
    LANGUAGE plpgsql
    AS $_$ 
DECLARE
	schema text;
BEGIN
	IF NOT LongTransactionsEnabled() THEN
		RAISE EXCEPTION 'Long transaction support disabled, use EnableLongTransaction() to enable.';
	END IF;

	if ( $1 != '' ) THEN
		schema = $1;
	ELSE
		SELECT current_schema() into schema;
	END IF;

	-- TODO: check for an already existing trigger ?

	EXECUTE 'CREATE TRIGGER check_auth BEFORE UPDATE OR DELETE ON ' 
		|| quote_ident(schema) || '.' || quote_ident($2)
		||' FOR EACH ROW EXECUTE PROCEDURE CheckAuthTrigger('
		|| quote_literal($3) || ')';

	RETURN 0;
END;
$_$;


ALTER FUNCTION public.checkauth(text, text, text) OWNER TO postgres;

--
-- Name: disablelongtransactions(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION disablelongtransactions() RETURNS text
    LANGUAGE plpgsql
    AS $$ 
DECLARE
	rec RECORD;

BEGIN

	--
	-- Drop all triggers applied by CheckAuth()
	--
	FOR rec IN
		SELECT c.relname, t.tgname, t.tgargs FROM pg_trigger t, pg_class c, pg_proc p
		WHERE p.proname = 'checkauthtrigger' and t.tgfoid = p.oid and t.tgrelid = c.oid
	LOOP
		EXECUTE 'DROP TRIGGER ' || quote_ident(rec.tgname) ||
			' ON ' || quote_ident(rec.relname);
	END LOOP;

	--
	-- Drop the authorization_table table
	--
	FOR rec IN SELECT * FROM pg_class WHERE relname = 'authorization_table' LOOP
		DROP TABLE authorization_table;
	END LOOP;

	--
	-- Drop the authorized_tables view
	--
	FOR rec IN SELECT * FROM pg_class WHERE relname = 'authorized_tables' LOOP
		DROP VIEW authorized_tables;
	END LOOP;

	RETURN 'Long transactions support disabled';
END;
$$;


ALTER FUNCTION public.disablelongtransactions() OWNER TO postgres;

--
-- Name: dropgeometrycolumn(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrycolumn(table_name character varying, column_name character varying) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $_$
DECLARE
	ret text;
BEGIN
	SELECT DropGeometryColumn('','',$1,$2) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.dropgeometrycolumn(table_name character varying, column_name character varying) OWNER TO postgres;

--
-- Name: dropgeometrycolumn(character varying, character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrycolumn(schema_name character varying, table_name character varying, column_name character varying) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $_$
DECLARE
	ret text;
BEGIN
	SELECT DropGeometryColumn('',$1,$2,$3) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.dropgeometrycolumn(schema_name character varying, table_name character varying, column_name character varying) OWNER TO postgres;

--
-- Name: dropgeometrycolumn(character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrycolumn(catalog_name character varying, schema_name character varying, table_name character varying, column_name character varying) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $$
DECLARE
	myrec RECORD;
	okay boolean;
	real_schema name;

BEGIN


	-- Find, check or fix schema_name
	IF ( schema_name != '' ) THEN
		okay = false;

		FOR myrec IN SELECT nspname FROM pg_namespace WHERE text(nspname) = schema_name LOOP
			okay := true;
		END LOOP;

		IF ( okay <>  true ) THEN
			RAISE NOTICE 'Invalid schema name - using current_schema()';
			SELECT current_schema() into real_schema;
		ELSE
			real_schema = schema_name;
		END IF;
	ELSE
		SELECT current_schema() into real_schema;
	END IF;

	-- Find out if the column is in the geometry_columns table
	okay = false;
	FOR myrec IN SELECT * from geometry_columns where f_table_schema = text(real_schema) and f_table_name = table_name and f_geometry_column = column_name LOOP
		okay := true;
	END LOOP;
	IF (okay <> true) THEN
		RAISE EXCEPTION 'column not found in geometry_columns table';
		RETURN false;
	END IF;

	-- Remove table column
	EXECUTE 'ALTER TABLE ' || quote_ident(real_schema) || '.' ||
		quote_ident(table_name) || ' DROP COLUMN ' ||
		quote_ident(column_name);

	RETURN real_schema || '.' || table_name || '.' || column_name ||' effectively removed.';

END;
$$;


ALTER FUNCTION public.dropgeometrycolumn(catalog_name character varying, schema_name character varying, table_name character varying, column_name character varying) OWNER TO postgres;

--
-- Name: dropgeometrytable(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrytable(table_name character varying) RETURNS text
    LANGUAGE sql STRICT
    AS $_$ SELECT DropGeometryTable('','',$1) $_$;


ALTER FUNCTION public.dropgeometrytable(table_name character varying) OWNER TO postgres;

--
-- Name: dropgeometrytable(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrytable(schema_name character varying, table_name character varying) RETURNS text
    LANGUAGE sql STRICT
    AS $_$ SELECT DropGeometryTable('',$1,$2) $_$;


ALTER FUNCTION public.dropgeometrytable(schema_name character varying, table_name character varying) OWNER TO postgres;

--
-- Name: dropgeometrytable(character varying, character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropgeometrytable(catalog_name character varying, schema_name character varying, table_name character varying) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $$
DECLARE
	real_schema name;

BEGIN

	IF ( schema_name = '' ) THEN
		SELECT current_schema() into real_schema;
	ELSE
		real_schema = schema_name;
	END IF;

	-- TODO: Should we warn if table doesn't exist probably instead just saying dropped
	-- Remove table
	EXECUTE 'DROP TABLE IF EXISTS '
		|| quote_ident(real_schema) || '.' ||
		quote_ident(table_name) || ' RESTRICT';

	RETURN
		real_schema || '.' ||
		table_name ||' dropped.';

END;
$$;


ALTER FUNCTION public.dropgeometrytable(catalog_name character varying, schema_name character varying, table_name character varying) OWNER TO postgres;

--
-- Name: dropoverviewconstraints(name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropoverviewconstraints(ovtable name, ovcolumn name) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT DropOverviewConstraints('', $1, $2) $_$;


ALTER FUNCTION public.dropoverviewconstraints(ovtable name, ovcolumn name) OWNER TO postgres;

--
-- Name: dropoverviewconstraints(name, name, name); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION dropoverviewconstraints(ovschema name, ovtable name, ovcolumn name) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		schema name;
		sql text;
		rtn boolean;
	BEGIN
		-- validate schema
		schema := NULL;
		IF length($1) > 0 THEN
			sql := 'SELECT nspname FROM pg_namespace '
				|| 'WHERE nspname = ' || quote_literal($1)
				|| 'LIMIT 1';
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The value provided for schema is invalid';
				RETURN FALSE;
			END IF;
		END IF;

		IF schema IS NULL THEN
			sql := 'SELECT n.nspname AS schemaname '
				|| 'FROM pg_catalog.pg_class c '
				|| 'JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
				|| 'WHERE c.relkind = ' || quote_literal('r')
				|| ' AND n.nspname NOT IN (' || quote_literal('pg_catalog')
				|| ', ' || quote_literal('pg_toast')
				|| ') AND pg_catalog.pg_table_is_visible(c.oid)'
				|| ' AND c.relname = ' || quote_literal($2);
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The table % does not occur in the search_path', quote_literal($2);
				RETURN FALSE;
			END IF;
		END IF;

		rtn := _drop_overview_constraint(schema, $2, $3);
		IF rtn IS FALSE THEN
			RAISE EXCEPTION 'Unable to drop the overview constraint .  Is the schema name, table name or column name incorrect?';
			RETURN FALSE;
		END IF;

		RETURN TRUE;
	END;
	$_$;


ALTER FUNCTION public.dropoverviewconstraints(ovschema name, ovtable name, ovcolumn name) OWNER TO postgres;

--
-- Name: droprasterconstraints(name, name, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION droprasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT DropRasterConstraints('', $1, $2, VARIADIC $3) $_$;


ALTER FUNCTION public.droprasterconstraints(rasttable name, rastcolumn name, VARIADIC constraints text[]) OWNER TO postgres;

--
-- Name: droprasterconstraints(name, name, name, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		max int;
		x int;
		schema name;
		sql text;
		kw text;
		rtn boolean;
		cnt int;
	BEGIN
		cnt := 0;
		max := array_length(constraints, 1);
		IF max < 1 THEN
			RAISE NOTICE 'No constraints indicated to be dropped.  Doing nothing';
			RETURN TRUE;
		END IF;

		-- validate schema
		schema := NULL;
		IF length($1) > 0 THEN
			sql := 'SELECT nspname FROM pg_namespace '
				|| 'WHERE nspname = ' || quote_literal($1)
				|| 'LIMIT 1';
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The value provided for schema is invalid';
				RETURN FALSE;
			END IF;
		END IF;

		IF schema IS NULL THEN
			sql := 'SELECT n.nspname AS schemaname '
				|| 'FROM pg_catalog.pg_class c '
				|| 'JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
				|| 'WHERE c.relkind = ' || quote_literal('r')
				|| ' AND n.nspname NOT IN (' || quote_literal('pg_catalog')
				|| ', ' || quote_literal('pg_toast')
				|| ') AND pg_catalog.pg_table_is_visible(c.oid)'
				|| ' AND c.relname = ' || quote_literal($2);
			EXECUTE sql INTO schema;

			IF schema IS NULL THEN
				RAISE EXCEPTION 'The table % does not occur in the search_path', quote_literal($2);
				RETURN FALSE;
			END IF;
		END IF;

		<<kwloop>>
		FOR x in 1..max LOOP
			kw := trim(both from lower(constraints[x]));

			BEGIN
				CASE
					WHEN kw = 'srid' THEN
						RAISE NOTICE 'Dropping SRID constraint';
						rtn := _drop_raster_constraint_srid(schema, $2, $3);
					WHEN kw IN ('scale_x', 'scalex') THEN
						RAISE NOTICE 'Dropping scale-X constraint';
						rtn := _drop_raster_constraint_scale(schema, $2, $3, 'x');
					WHEN kw IN ('scale_y', 'scaley') THEN
						RAISE NOTICE 'Dropping scale-Y constraint';
						rtn := _drop_raster_constraint_scale(schema, $2, $3, 'y');
					WHEN kw = 'scale' THEN
						RAISE NOTICE 'Dropping scale-X constraint';
						rtn := _drop_raster_constraint_scale(schema, $2, $3, 'x');
						RAISE NOTICE 'Dropping scale-Y constraint';
						rtn := _drop_raster_constraint_scale(schema, $2, $3, 'y');
					WHEN kw IN ('blocksize_x', 'blocksizex', 'width') THEN
						RAISE NOTICE 'Dropping blocksize-X constraint';
						rtn := _drop_raster_constraint_blocksize(schema, $2, $3, 'width');
					WHEN kw IN ('blocksize_y', 'blocksizey', 'height') THEN
						RAISE NOTICE 'Dropping blocksize-Y constraint';
						rtn := _drop_raster_constraint_blocksize(schema, $2, $3, 'height');
					WHEN kw = 'blocksize' THEN
						RAISE NOTICE 'Dropping blocksize-X constraint';
						rtn := _drop_raster_constraint_blocksize(schema, $2, $3, 'width');
						RAISE NOTICE 'Dropping blocksize-Y constraint';
						rtn := _drop_raster_constraint_blocksize(schema, $2, $3, 'height');
					WHEN kw IN ('same_alignment', 'samealignment', 'alignment') THEN
						RAISE NOTICE 'Dropping alignment constraint';
						rtn := _drop_raster_constraint_alignment(schema, $2, $3);
					WHEN kw IN ('regular_blocking', 'regularblocking') THEN
						RAISE NOTICE 'Dropping regular blocking constraint';
						rtn := _drop_raster_constraint_regular_blocking(schema, $2, $3);
					WHEN kw IN ('num_bands', 'numbands') THEN
						RAISE NOTICE 'Dropping number of bands constraint';
						rtn := _drop_raster_constraint_num_bands(schema, $2, $3);
					WHEN kw IN ('pixel_types', 'pixeltypes') THEN
						RAISE NOTICE 'Dropping pixel type constraint';
						rtn := _drop_raster_constraint_pixel_types(schema, $2, $3);
					WHEN kw IN ('nodata_values', 'nodatavalues', 'nodata') THEN
						RAISE NOTICE 'Dropping nodata value constraint';
						rtn := _drop_raster_constraint_nodata_values(schema, $2, $3);
					WHEN kw IN ('out_db', 'outdb') THEN
						RAISE NOTICE 'Dropping out-of-database constraint';
						rtn := _drop_raster_constraint_out_db(schema, $2, $3);
					WHEN kw = 'extent' THEN
						RAISE NOTICE 'Dropping maximum extent constraint';
						rtn := _drop_raster_constraint_extent(schema, $2, $3);
					ELSE
						RAISE NOTICE 'Unknown constraint: %.  Skipping', quote_literal(constraints[x]);
						CONTINUE kwloop;
				END CASE;
			END;

			IF rtn IS FALSE THEN
				cnt := cnt + 1;
				RAISE WARNING 'Unable to drop constraint: %.  Skipping', quote_literal(constraints[x]);
			END IF;

		END LOOP kwloop;

		IF cnt = max THEN
			RAISE EXCEPTION 'None of the constraints specified could be dropped.  Is the schema name, table name or column name incorrect?';
			RETURN FALSE;
		END IF;

		RETURN TRUE;
	END;
	$_$;


ALTER FUNCTION public.droprasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) OWNER TO postgres;

--
-- Name: FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, VARIADIC constraints text[]) IS 'args: rastschema, rasttable, rastcolumn, constraints - Drops PostGIS raster constraints that refer to a raster table column. Useful if you need to reload data or update your raster column data.';


--
-- Name: droprasterconstraints(name, name, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION droprasterconstraints(rasttable name, rastcolumn name, srid boolean DEFAULT true, scale_x boolean DEFAULT true, scale_y boolean DEFAULT true, blocksize_x boolean DEFAULT true, blocksize_y boolean DEFAULT true, same_alignment boolean DEFAULT true, regular_blocking boolean DEFAULT true, num_bands boolean DEFAULT true, pixel_types boolean DEFAULT true, nodata_values boolean DEFAULT true, out_db boolean DEFAULT true, extent boolean DEFAULT true) RETURNS boolean
    LANGUAGE sql STRICT
    AS $_$ SELECT DropRasterConstraints('', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) $_$;


ALTER FUNCTION public.droprasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) OWNER TO postgres;

--
-- Name: FUNCTION droprasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION droprasterconstraints(rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) IS 'args: rasttable, rastcolumn, srid, scale_x, scale_y, blocksize_x, blocksize_y, same_alignment, regular_blocking, num_bands=true, pixel_types=true, nodata_values=true, out_db=true, extent=true - Drops PostGIS raster constraints that refer to a raster table column. Useful if you need to reload data or update your raster column data.';


--
-- Name: droprasterconstraints(name, name, name, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean DEFAULT true, scale_x boolean DEFAULT true, scale_y boolean DEFAULT true, blocksize_x boolean DEFAULT true, blocksize_y boolean DEFAULT true, same_alignment boolean DEFAULT true, regular_blocking boolean DEFAULT true, num_bands boolean DEFAULT true, pixel_types boolean DEFAULT true, nodata_values boolean DEFAULT true, out_db boolean DEFAULT true, extent boolean DEFAULT true) RETURNS boolean
    LANGUAGE plpgsql STRICT
    AS $_$
	DECLARE
		constraints text[];
	BEGIN
		IF srid IS TRUE THEN
			constraints := constraints || 'srid'::text;
		END IF;

		IF scale_x IS TRUE THEN
			constraints := constraints || 'scale_x'::text;
		END IF;

		IF scale_y IS TRUE THEN
			constraints := constraints || 'scale_y'::text;
		END IF;

		IF blocksize_x IS TRUE THEN
			constraints := constraints || 'blocksize_x'::text;
		END IF;

		IF blocksize_y IS TRUE THEN
			constraints := constraints || 'blocksize_y'::text;
		END IF;

		IF same_alignment IS TRUE THEN
			constraints := constraints || 'same_alignment'::text;
		END IF;

		IF regular_blocking IS TRUE THEN
			constraints := constraints || 'regular_blocking'::text;
		END IF;

		IF num_bands IS TRUE THEN
			constraints := constraints || 'num_bands'::text;
		END IF;

		IF pixel_types IS TRUE THEN
			constraints := constraints || 'pixel_types'::text;
		END IF;

		IF nodata_values IS TRUE THEN
			constraints := constraints || 'nodata_values'::text;
		END IF;

		IF out_db IS TRUE THEN
			constraints := constraints || 'out_db'::text;
		END IF;

		IF extent IS TRUE THEN
			constraints := constraints || 'extent'::text;
		END IF;

		RETURN DropRasterConstraints($1, $2, $3, VARIADIC constraints);
	END;
	$_$;


ALTER FUNCTION public.droprasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) OWNER TO postgres;

--
-- Name: FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION droprasterconstraints(rastschema name, rasttable name, rastcolumn name, srid boolean, scale_x boolean, scale_y boolean, blocksize_x boolean, blocksize_y boolean, same_alignment boolean, regular_blocking boolean, num_bands boolean, pixel_types boolean, nodata_values boolean, out_db boolean, extent boolean) IS 'args: rastschema, rasttable, rastcolumn, srid=true, scale_x=true, scale_y=true, blocksize_x=true, blocksize_y=true, same_alignment=true, regular_blocking=true, num_bands=true, pixel_types=true, nodata_values=true, out_db=true, extent=true - Drops PostGIS raster constraints that refer to a raster table column. Useful if you need to reload data or update your raster column data.';


--
-- Name: enablelongtransactions(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION enablelongtransactions() RETURNS text
    LANGUAGE plpgsql
    AS $$ 
DECLARE
	"query" text;
	exists bool;
	rec RECORD;

BEGIN

	exists = 'f';
	FOR rec IN SELECT * FROM pg_class WHERE relname = 'authorization_table'
	LOOP
		exists = 't';
	END LOOP;

	IF NOT exists
	THEN
		"query" = 'CREATE TABLE authorization_table (
			toid oid, -- table oid
			rid text, -- row id
			expires timestamp,
			authid text
		)';
		EXECUTE "query";
	END IF;

	exists = 'f';
	FOR rec IN SELECT * FROM pg_class WHERE relname = 'authorized_tables'
	LOOP
		exists = 't';
	END LOOP;

	IF NOT exists THEN
		"query" = 'CREATE VIEW authorized_tables AS ' ||
			'SELECT ' ||
			'n.nspname as schema, ' ||
			'c.relname as table, trim(' ||
			quote_literal(chr(92) || '000') ||
			' from t.tgargs) as id_column ' ||
			'FROM pg_trigger t, pg_class c, pg_proc p ' ||
			', pg_namespace n ' ||
			'WHERE p.proname = ' || quote_literal('checkauthtrigger') ||
			' AND c.relnamespace = n.oid' ||
			' AND t.tgfoid = p.oid and t.tgrelid = c.oid';
		EXECUTE "query";
	END IF;

	RETURN 'Long transactions support enabled';
END;
$$;


ALTER FUNCTION public.enablelongtransactions() OWNER TO postgres;

--
-- Name: find_srid(character varying, character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION find_srid(character varying, character varying, character varying) RETURNS integer
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $_$
DECLARE
	schem text;
	tabl text;
	sr int4;
BEGIN
	IF $1 IS NULL THEN
	  RAISE EXCEPTION 'find_srid() - schema is NULL!';
	END IF;
	IF $2 IS NULL THEN
	  RAISE EXCEPTION 'find_srid() - table name is NULL!';
	END IF;
	IF $3 IS NULL THEN
	  RAISE EXCEPTION 'find_srid() - column name is NULL!';
	END IF;
	schem = $1;
	tabl = $2;
-- if the table contains a . and the schema is empty
-- split the table into a schema and a table
-- otherwise drop through to default behavior
	IF ( schem = '' and tabl LIKE '%.%' ) THEN
	 schem = substr(tabl,1,strpos(tabl,'.')-1);
	 tabl = substr(tabl,length(schem)+2);
	ELSE
	 schem = schem || '%';
	END IF;

	select SRID into sr from geometry_columns where f_table_schema like schem and f_table_name = tabl and f_geometry_column = $3;
	IF NOT FOUND THEN
	   RAISE EXCEPTION 'find_srid() - couldnt find the corresponding SRID - is the geometry registered in the GEOMETRY_COLUMNS table?  Is there an uppercase/lowercase missmatch?';
	END IF;
	return sr;
END;
$_$;


ALTER FUNCTION public.find_srid(character varying, character varying, character varying) OWNER TO postgres;

--
-- Name: get_proj4_from_srid(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION get_proj4_from_srid(integer) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $_$
BEGIN
	RETURN proj4text::text FROM spatial_ref_sys WHERE srid= $1;
END;
$_$;


ALTER FUNCTION public.get_proj4_from_srid(integer) OWNER TO postgres;

--
-- Name: lockrow(text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION lockrow(text, text, text) RETURNS integer
    LANGUAGE sql STRICT
    AS $_$ SELECT LockRow(current_schema(), $1, $2, $3, now()::timestamp+'1:00'); $_$;


ALTER FUNCTION public.lockrow(text, text, text) OWNER TO postgres;

--
-- Name: lockrow(text, text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION lockrow(text, text, text, text) RETURNS integer
    LANGUAGE sql STRICT
    AS $_$ SELECT LockRow($1, $2, $3, $4, now()::timestamp+'1:00'); $_$;


ALTER FUNCTION public.lockrow(text, text, text, text) OWNER TO postgres;

--
-- Name: lockrow(text, text, text, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION lockrow(text, text, text, timestamp without time zone) RETURNS integer
    LANGUAGE sql STRICT
    AS $_$ SELECT LockRow(current_schema(), $1, $2, $3, $4); $_$;


ALTER FUNCTION public.lockrow(text, text, text, timestamp without time zone) OWNER TO postgres;

--
-- Name: lockrow(text, text, text, text, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION lockrow(text, text, text, text, timestamp without time zone) RETURNS integer
    LANGUAGE plpgsql STRICT
    AS $_$ 
DECLARE
	myschema alias for $1;
	mytable alias for $2;
	myrid   alias for $3;
	authid alias for $4;
	expires alias for $5;
	ret int;
	mytoid oid;
	myrec RECORD;
	
BEGIN

	IF NOT LongTransactionsEnabled() THEN
		RAISE EXCEPTION 'Long transaction support disabled, use EnableLongTransaction() to enable.';
	END IF;

	EXECUTE 'DELETE FROM authorization_table WHERE expires < now()'; 

	SELECT c.oid INTO mytoid FROM pg_class c, pg_namespace n
		WHERE c.relname = mytable
		AND c.relnamespace = n.oid
		AND n.nspname = myschema;

	-- RAISE NOTICE 'toid: %', mytoid;

	FOR myrec IN SELECT * FROM authorization_table WHERE 
		toid = mytoid AND rid = myrid
	LOOP
		IF myrec.authid != authid THEN
			RETURN 0;
		ELSE
			RETURN 1;
		END IF;
	END LOOP;

	EXECUTE 'INSERT INTO authorization_table VALUES ('||
		quote_literal(mytoid::text)||','||quote_literal(myrid)||
		','||quote_literal(expires::text)||
		','||quote_literal(authid) ||')';

	GET DIAGNOSTICS ret = ROW_COUNT;

	RETURN ret;
END;
$_$;


ALTER FUNCTION public.lockrow(text, text, text, text, timestamp without time zone) OWNER TO postgres;

--
-- Name: longtransactionsenabled(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION longtransactionsenabled() RETURNS boolean
    LANGUAGE plpgsql
    AS $$ 
DECLARE
	rec RECORD;
BEGIN
	FOR rec IN SELECT oid FROM pg_class WHERE relname = 'authorized_tables'
	LOOP
		return 't';
	END LOOP;
	return 'f';
END;
$$;


ALTER FUNCTION public.longtransactionsenabled() OWNER TO postgres;

--
-- Name: populate_geometry_columns(boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION populate_geometry_columns(use_typmod boolean DEFAULT true) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
	inserted    integer;
	oldcount    integer;
	probed      integer;
	stale       integer;
	gcs         RECORD;
	gc          RECORD;
	gsrid       integer;
	gndims      integer;
	gtype       text;
	query       text;
	gc_is_valid boolean;

BEGIN
	SELECT count(*) INTO oldcount FROM geometry_columns;
	inserted := 0;

	-- Count the number of geometry columns in all tables and views
	SELECT count(DISTINCT c.oid) INTO probed
	FROM pg_class c,
		 pg_attribute a,
		 pg_type t,
		 pg_namespace n
	WHERE (c.relkind = 'r' OR c.relkind = 'v')
		AND t.typname = 'geometry'
		AND a.attisdropped = false
		AND a.atttypid = t.oid
		AND a.attrelid = c.oid
		AND c.relnamespace = n.oid
		AND n.nspname NOT ILIKE 'pg_temp%' AND c.relname != 'raster_columns' ;

	-- Iterate through all non-dropped geometry columns
	RAISE DEBUG 'Processing Tables.....';

	FOR gcs IN
	SELECT DISTINCT ON (c.oid) c.oid, n.nspname, c.relname
		FROM pg_class c,
			 pg_attribute a,
			 pg_type t,
			 pg_namespace n
		WHERE c.relkind = 'r'
		AND t.typname = 'geometry'
		AND a.attisdropped = false
		AND a.atttypid = t.oid
		AND a.attrelid = c.oid
		AND c.relnamespace = n.oid
		AND n.nspname NOT ILIKE 'pg_temp%' AND c.relname != 'raster_columns' 
	LOOP

		inserted := inserted + populate_geometry_columns(gcs.oid, use_typmod);
	END LOOP;

	IF oldcount > inserted THEN
	    stale = oldcount-inserted;
	ELSE
	    stale = 0;
	END IF;

	RETURN 'probed:' ||probed|| ' inserted:'||inserted;
END

$$;


ALTER FUNCTION public.populate_geometry_columns(use_typmod boolean) OWNER TO postgres;

--
-- Name: populate_geometry_columns(oid, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION populate_geometry_columns(tbl_oid oid, use_typmod boolean DEFAULT true) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
	gcs         RECORD;
	gc          RECORD;
	gc_old      RECORD;
	gsrid       integer;
	gndims      integer;
	gtype       text;
	query       text;
	gc_is_valid boolean;
	inserted    integer;
	constraint_successful boolean := false;

BEGIN
	inserted := 0;

	-- Iterate through all geometry columns in this table
	FOR gcs IN
	SELECT n.nspname, c.relname, a.attname
		FROM pg_class c,
			 pg_attribute a,
			 pg_type t,
			 pg_namespace n
		WHERE c.relkind = 'r'
		AND t.typname = 'geometry'
		AND a.attisdropped = false
		AND a.atttypid = t.oid
		AND a.attrelid = c.oid
		AND c.relnamespace = n.oid
		AND n.nspname NOT ILIKE 'pg_temp%'
		AND c.oid = tbl_oid
	LOOP

        RAISE DEBUG 'Processing table %.%.%', gcs.nspname, gcs.relname, gcs.attname;
    
        gc_is_valid := true;
        -- Find the srid, coord_dimension, and type of current geometry
        -- in geometry_columns -- which is now a view
        
        SELECT type, srid, coord_dimension INTO gc_old 
            FROM geometry_columns 
            WHERE f_table_schema = gcs.nspname AND f_table_name = gcs.relname AND f_geometry_column = gcs.attname; 
            
        IF upper(gc_old.type) = 'GEOMETRY' THEN
        -- This is an unconstrained geometry we need to do something
        -- We need to figure out what to set the type by inspecting the data
            EXECUTE 'SELECT st_srid(' || quote_ident(gcs.attname) || ') As srid, GeometryType(' || quote_ident(gcs.attname) || ') As type, ST_NDims(' || quote_ident(gcs.attname) || ') As dims ' ||
                     ' FROM ONLY ' || quote_ident(gcs.nspname) || '.' || quote_ident(gcs.relname) || 
                     ' WHERE ' || quote_ident(gcs.attname) || ' IS NOT NULL LIMIT 1;'
                INTO gc;
            IF gc IS NULL THEN -- there is no data so we can not determine geometry type
            	RAISE WARNING 'No data in table %.%, so no information to determine geometry type and srid', gcs.nspname, gcs.relname;
            	RETURN 0;
            END IF;
            gsrid := gc.srid; gtype := gc.type; gndims := gc.dims;
            	
            IF use_typmod THEN
                BEGIN
                    EXECUTE 'ALTER TABLE ' || quote_ident(gcs.nspname) || '.' || quote_ident(gcs.relname) || ' ALTER COLUMN ' || quote_ident(gcs.attname) || 
                        ' TYPE geometry(' || postgis_type_name(gtype, gndims, true) || ', ' || gsrid::text  || ') ';
                    inserted := inserted + 1;
                EXCEPTION
                        WHEN invalid_parameter_value THEN
                        RAISE WARNING 'Could not convert ''%'' in ''%.%'' to use typmod with srid %, type: % ', quote_ident(gcs.attname), quote_ident(gcs.nspname), quote_ident(gcs.relname), gsrid, postgis_type_name(gtype, gndims, true);
                            gc_is_valid := false;
                END;
                
            ELSE
                -- Try to apply srid check to column
            	constraint_successful = false;
                IF (gsrid > 0 AND postgis_constraint_srid(gcs.nspname, gcs.relname,gcs.attname) IS NULL ) THEN
                    BEGIN
                        EXECUTE 'ALTER TABLE ONLY ' || quote_ident(gcs.nspname) || '.' || quote_ident(gcs.relname) || 
                                 ' ADD CONSTRAINT ' || quote_ident('enforce_srid_' || gcs.attname) || 
                                 ' CHECK (st_srid(' || quote_ident(gcs.attname) || ') = ' || gsrid || ')';
                        constraint_successful := true;
                    EXCEPTION
                        WHEN check_violation THEN
                            RAISE WARNING 'Not inserting ''%'' in ''%.%'' into geometry_columns: could not apply constraint CHECK (st_srid(%) = %)', quote_ident(gcs.attname), quote_ident(gcs.nspname), quote_ident(gcs.relname), quote_ident(gcs.attname), gsrid;
                            gc_is_valid := false;
                    END;
                END IF;
                
                -- Try to apply ndims check to column
                IF (gndims IS NOT NULL AND postgis_constraint_dims(gcs.nspname, gcs.relname,gcs.attname) IS NULL ) THEN
                    BEGIN
                        EXECUTE 'ALTER TABLE ONLY ' || quote_ident(gcs.nspname) || '.' || quote_ident(gcs.relname) || '
                                 ADD CONSTRAINT ' || quote_ident('enforce_dims_' || gcs.attname) || '
                                 CHECK (st_ndims(' || quote_ident(gcs.attname) || ') = '||gndims||')';
                        constraint_successful := true;
                    EXCEPTION
                        WHEN check_violation THEN
                            RAISE WARNING 'Not inserting ''%'' in ''%.%'' into geometry_columns: could not apply constraint CHECK (st_ndims(%) = %)', quote_ident(gcs.attname), quote_ident(gcs.nspname), quote_ident(gcs.relname), quote_ident(gcs.attname), gndims;
                            gc_is_valid := false;
                    END;
                END IF;
    
                -- Try to apply geometrytype check to column
                IF (gtype IS NOT NULL AND postgis_constraint_type(gcs.nspname, gcs.relname,gcs.attname) IS NULL ) THEN
                    BEGIN
                        EXECUTE 'ALTER TABLE ONLY ' || quote_ident(gcs.nspname) || '.' || quote_ident(gcs.relname) || '
                        ADD CONSTRAINT ' || quote_ident('enforce_geotype_' || gcs.attname) || '
                        CHECK ((geometrytype(' || quote_ident(gcs.attname) || ') = ' || quote_literal(gtype) || ') OR (' || quote_ident(gcs.attname) || ' IS NULL))';
                        constraint_successful := true;
                    EXCEPTION
                        WHEN check_violation THEN
                            -- No geometry check can be applied. This column contains a number of geometry types.
                            RAISE WARNING 'Could not add geometry type check (%) to table column: %.%.%', gtype, quote_ident(gcs.nspname),quote_ident(gcs.relname),quote_ident(gcs.attname);
                    END;
                END IF;
                 --only count if we were successful in applying at least one constraint
                IF constraint_successful THEN
                	inserted := inserted + 1;
                END IF;
            END IF;	        
	    END IF;

	END LOOP;

	RETURN inserted;
END

$$;


ALTER FUNCTION public.populate_geometry_columns(tbl_oid oid, use_typmod boolean) OWNER TO postgres;

--
-- Name: postgis_constraint_dims(text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_constraint_dims(geomschema text, geomtable text, geomcolumn text) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$
SELECT  replace(split_part(s.consrc, ' = ', 2), ')', '')::integer
		 FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
		 WHERE n.nspname = $1
		 AND c.relname = $2
		 AND a.attname = $3
		 AND a.attrelid = c.oid
		 AND s.connamespace = n.oid
		 AND s.conrelid = c.oid
		 AND a.attnum = ANY (s.conkey)
		 AND s.consrc LIKE '%ndims(% = %';
$_$;


ALTER FUNCTION public.postgis_constraint_dims(geomschema text, geomtable text, geomcolumn text) OWNER TO postgres;

--
-- Name: postgis_constraint_srid(text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_constraint_srid(geomschema text, geomtable text, geomcolumn text) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$
SELECT replace(replace(split_part(s.consrc, ' = ', 2), ')', ''), '(', '')::integer
		 FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
		 WHERE n.nspname = $1
		 AND c.relname = $2
		 AND a.attname = $3
		 AND a.attrelid = c.oid
		 AND s.connamespace = n.oid
		 AND s.conrelid = c.oid
		 AND a.attnum = ANY (s.conkey)
		 AND s.consrc LIKE '%srid(% = %';
$_$;


ALTER FUNCTION public.postgis_constraint_srid(geomschema text, geomtable text, geomcolumn text) OWNER TO postgres;

--
-- Name: postgis_constraint_type(text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_constraint_type(geomschema text, geomtable text, geomcolumn text) RETURNS character varying
    LANGUAGE sql STABLE STRICT
    AS $_$
SELECT  replace(split_part(s.consrc, '''', 2), ')', '')::varchar		
		 FROM pg_class c, pg_namespace n, pg_attribute a, pg_constraint s
		 WHERE n.nspname = $1
		 AND c.relname = $2
		 AND a.attname = $3
		 AND a.attrelid = c.oid
		 AND s.connamespace = n.oid
		 AND s.conrelid = c.oid
		 AND a.attnum = ANY (s.conkey)
		 AND s.consrc LIKE '%geometrytype(% = %';
$_$;


ALTER FUNCTION public.postgis_constraint_type(geomschema text, geomtable text, geomcolumn text) OWNER TO postgres;

--
-- Name: postgis_full_version(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_full_version() RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
	libver text;
	svnver text;
	projver text;
	geosver text;
	gdalver text;
	libxmlver text;
	dbproc text;
	relproc text;
	fullver text;
	rast_lib_ver text;
	rast_scr_ver text;
	topo_scr_ver text;
	json_lib_ver text;
BEGIN
	SELECT postgis_lib_version() INTO libver;
	SELECT postgis_proj_version() INTO projver;
	SELECT postgis_geos_version() INTO geosver;
	SELECT postgis_libjson_version() INTO json_lib_ver;
	BEGIN
		SELECT postgis_gdal_version() INTO gdalver;
	EXCEPTION
		WHEN undefined_function THEN
			gdalver := NULL;
			RAISE NOTICE 'Function postgis_gdal_version() not found.  Is raster support enabled and rtpostgis.sql installed?';
	END;
	SELECT postgis_libxml_version() INTO libxmlver;
	SELECT postgis_scripts_installed() INTO dbproc;
	SELECT postgis_scripts_released() INTO relproc;
	select postgis_svn_version() INTO svnver;
	BEGIN
		SELECT postgis_topology_scripts_installed() INTO topo_scr_ver;
	EXCEPTION
		WHEN undefined_function THEN
			topo_scr_ver := NULL;
			RAISE NOTICE 'Function postgis_topology_scripts_installed() not found. Is topology support enabled and topology.sql installed?';
	END;

	BEGIN
		SELECT postgis_raster_scripts_installed() INTO rast_scr_ver;
	EXCEPTION
		WHEN undefined_function THEN
			rast_scr_ver := NULL;
			RAISE NOTICE 'Function postgis_raster_scripts_installed() not found. Is raster support enabled and rtpostgis.sql installed?';
	END;

	BEGIN
		SELECT postgis_raster_lib_version() INTO rast_lib_ver;
	EXCEPTION
		WHEN undefined_function THEN
			rast_lib_ver := NULL;
			RAISE NOTICE 'Function postgis_raster_lib_version() not found. Is raster support enabled and rtpostgis.sql installed?';
	END;

	fullver = 'POSTGIS="' || libver;

	IF  svnver IS NOT NULL THEN
		fullver = fullver || ' r' || svnver;
	END IF;

	fullver = fullver || '"';

	IF  geosver IS NOT NULL THEN
		fullver = fullver || ' GEOS="' || geosver || '"';
	END IF;

	IF  projver IS NOT NULL THEN
		fullver = fullver || ' PROJ="' || projver || '"';
	END IF;

	IF  gdalver IS NOT NULL THEN
		fullver = fullver || ' GDAL="' || gdalver || '"';
	END IF;

	IF  libxmlver IS NOT NULL THEN
		fullver = fullver || ' LIBXML="' || libxmlver || '"';
	END IF;

	IF json_lib_ver IS NOT NULL THEN
		fullver = fullver || ' LIBJSON="' || json_lib_ver || '"';
	END IF;

	-- fullver = fullver || ' DBPROC="' || dbproc || '"';
	-- fullver = fullver || ' RELPROC="' || relproc || '"';

	IF dbproc != relproc THEN
		fullver = fullver || ' (core procs from "' || dbproc || '" need upgrade)';
	END IF;

	IF topo_scr_ver IS NOT NULL THEN
		fullver = fullver || ' TOPOLOGY';
		IF topo_scr_ver != relproc THEN
			fullver = fullver || ' (topology procs from "' || topo_scr_ver || '" need upgrade)';
		END IF;
	END IF;

	IF rast_lib_ver IS NOT NULL THEN
		fullver = fullver || ' RASTER';
		IF rast_lib_ver != relproc THEN
			fullver = fullver || ' (raster lib from "' || rast_lib_ver || '" need upgrade)';
		END IF;
	END IF;

	IF rast_scr_ver IS NOT NULL AND rast_scr_ver != relproc THEN
		fullver = fullver || ' (raster procs from "' || rast_scr_ver || '" need upgrade)';
	END IF;

	RETURN fullver;
END
$$;


ALTER FUNCTION public.postgis_full_version() OWNER TO postgres;

--
-- Name: postgis_raster_scripts_installed(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_raster_scripts_installed() RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT '2.0.1'::text || ' r' || 9979::text AS version $$;


ALTER FUNCTION public.postgis_raster_scripts_installed() OWNER TO postgres;

--
-- Name: postgis_scripts_build_date(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_scripts_build_date() RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$SELECT '2012-11-16 18:39:39'::text AS version$$;


ALTER FUNCTION public.postgis_scripts_build_date() OWNER TO postgres;

--
-- Name: postgis_scripts_installed(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_scripts_installed() RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT '2.0.1'::text || ' r' || 9979::text AS version $$;


ALTER FUNCTION public.postgis_scripts_installed() OWNER TO postgres;

--
-- Name: postgis_topology_scripts_installed(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_topology_scripts_installed() RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT '2.0.1'::text || ' r' || 9979::text AS version $$;


ALTER FUNCTION public.postgis_topology_scripts_installed() OWNER TO postgres;

--
-- Name: postgis_type_name(character varying, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION postgis_type_name(geomname character varying, coord_dimension integer, use_new_name boolean DEFAULT true) RETURNS character varying
    LANGUAGE sql IMMUTABLE STRICT COST 200
    AS $_$
 SELECT CASE WHEN $3 THEN new_name ELSE old_name END As geomname
 	FROM 
 	( VALUES
 		 ('GEOMETRY', 'Geometry', 2) ,
 		 	('GEOMETRY', 'GeometryZ', 3) ,
 		 	('GEOMETRY', 'GeometryZM', 4) ,
			('GEOMETRYCOLLECTION', 'GeometryCollection', 2) ,
			('GEOMETRYCOLLECTION', 'GeometryCollectionZ', 3) ,
			('GEOMETRYCOLLECTIONM', 'GeometryCollectionM', 3) ,
			('GEOMETRYCOLLECTION', 'GeometryCollectionZM', 4) ,
			
			('POINT', 'Point',2) ,
			('POINTM','PointM',3) ,
			('POINT', 'PointZ',3) ,
			('POINT', 'PointZM',4) ,
			
			('MULTIPOINT','MultiPoint',2) ,
			('MULTIPOINT','MultiPointZ',3) ,
			('MULTIPOINTM','MultiPointM',3) ,
			('MULTIPOINT','MultiPointZM',4) ,
			
			('POLYGON', 'Polygon',2) ,
			('POLYGON', 'PolygonZ',3) ,
			('POLYGONM', 'PolygonM',3) ,
			('POLYGON', 'PolygonZM',4) ,
			
			('MULTIPOLYGON', 'MultiPolygon',2) ,
			('MULTIPOLYGON', 'MultiPolygonZ',3) ,
			('MULTIPOLYGONM', 'MultiPolygonM',3) ,
			('MULTIPOLYGON', 'MultiPolygonZM',4) ,
			
			('MULTILINESTRING', 'MultiLineString',2) ,
			('MULTILINESTRING', 'MultiLineStringZ',3) ,
			('MULTILINESTRINGM', 'MultiLineStringM',3) ,
			('MULTILINESTRING', 'MultiLineStringZM',4) ,
			
			('LINESTRING', 'LineString',2) ,
			('LINESTRING', 'LineStringZ',3) ,
			('LINESTRINGM', 'LineStringM',3) ,
			('LINESTRING', 'LineStringZM',4) ,
			
			('CIRCULARSTRING', 'CircularString',2) ,
			('CIRCULARSTRING', 'CircularStringZ',3) ,
			('CIRCULARSTRINGM', 'CircularStringM',3) ,
			('CIRCULARSTRING', 'CircularStringZM',4) ,
			
			('COMPOUNDCURVE', 'CompoundCurve',2) ,
			('COMPOUNDCURVE', 'CompoundCurveZ',3) ,
			('COMPOUNDCURVEM', 'CompoundCurveM',3) ,
			('COMPOUNDCURVE', 'CompoundCurveZM',4) ,
			
			('CURVEPOLYGON', 'CurvePolygon',2) ,
			('CURVEPOLYGON', 'CurvePolygonZ',3) ,
			('CURVEPOLYGONM', 'CurvePolygonM',3) ,
			('CURVEPOLYGON', 'CurvePolygonZM',4) ,
			
			('MULTICURVE', 'MultiCurve',2 ) ,
			('MULTICURVE', 'MultiCurveZ',3 ) ,
			('MULTICURVEM', 'MultiCurveM',3 ) ,
			('MULTICURVE', 'MultiCurveZM',4 ) ,
			
			('MULTISURFACE', 'MultiSurface', 2) ,
			('MULTISURFACE', 'MultiSurfaceZ', 3) ,
			('MULTISURFACEM', 'MultiSurfaceM', 3) ,
			('MULTISURFACE', 'MultiSurfaceZM', 4) ,
			
			('POLYHEDRALSURFACE', 'PolyhedralSurface',2) ,
			('POLYHEDRALSURFACE', 'PolyhedralSurfaceZ',3) ,
			('POLYHEDRALSURFACEM', 'PolyhedralSurfaceM',3) ,
			('POLYHEDRALSURFACE', 'PolyhedralSurfaceZM',4) ,
			
			('TRIANGLE', 'Triangle',2) ,
			('TRIANGLE', 'TriangleZ',3) ,
			('TRIANGLEM', 'TriangleM',3) ,
			('TRIANGLE', 'TriangleZM',4) ,

			('TIN', 'Tin', 2),
			('TIN', 'TinZ', 3),
			('TIN', 'TinM', 3),
			('TIN', 'TinZM', 4) )
			 As g(old_name, new_name, coord_dimension)
		WHERE (upper(old_name) = upper($1) OR upper(new_name) = upper($1))
			AND coord_dimension = $2;
$_$;


ALTER FUNCTION public.postgis_type_name(geomname character varying, coord_dimension integer, use_new_name boolean) OWNER TO postgres;

--
-- Name: st_approxcount(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxcount(rastertable text, rastercolumn text, sample_percent double precision) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, 1, TRUE, $3) $_$;


ALTER FUNCTION public.st_approxcount(rastertable text, rastercolumn text, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxcount(text, text, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxcount(rastertable text, rastercolumn text, exclude_nodata_value boolean, sample_percent double precision DEFAULT 0.1) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, 1, $3, $4) $_$;


ALTER FUNCTION public.st_approxcount(rastertable text, rastercolumn text, exclude_nodata_value boolean, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxcount(text, text, integer, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxcount(rastertable text, rastercolumn text, nband integer, sample_percent double precision) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, $3, TRUE, $4) $_$;


ALTER FUNCTION public.st_approxcount(rastertable text, rastercolumn text, nband integer, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxcount(text, text, integer, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxcount(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, sample_percent double precision DEFAULT 0.1) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, $3, $4, $5) $_$;


ALTER FUNCTION public.st_approxcount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, sample_percent double precision) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, 1, TRUE, $3, 0, NULL, FALSE) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, integer, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, TRUE, $4, 0, NULL, FALSE) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, integer, double precision, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision, bins integer, "right" boolean) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, TRUE, $4, $5, NULL, $6) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision, bins integer, "right" boolean) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, integer, boolean, double precision, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, bins integer, "right" boolean) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, $4, $5, $6, NULL, $7) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, bins integer, "right" boolean) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, integer, double precision, integer, double precision[], boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision, bins integer, width double precision[] DEFAULT NULL::double precision[], "right" boolean DEFAULT false) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, TRUE, $4, $5, $6, $7) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, nband integer, sample_percent double precision, bins integer, width double precision[], "right" boolean) OWNER TO postgres;

--
-- Name: st_approxhistogram(text, text, integer, boolean, double precision, integer, double precision[], boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxhistogram(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, sample_percent double precision DEFAULT 0.1, bins integer DEFAULT 0, width double precision[] DEFAULT NULL::double precision[], "right" boolean DEFAULT false) RETURNS SETOF histogram
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_histogram($1, $2, $3, $4, $5, $6, $7, $8) $_$;


ALTER FUNCTION public.st_approxhistogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, bins integer, width double precision[], "right" boolean) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, quantiles double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_quantile($1, $2, 1, TRUE, 0.1, $3) $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, quantiles double precision[]) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE
    AS $_$ SELECT (_st_quantile($1, $2, 1, TRUE, 0.1, ARRAY[$3]::double precision[])).value $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, quantile double precision) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision DEFAULT NULL::double precision) RETURNS double precision
    LANGUAGE sql STABLE
    AS $_$ SELECT (_st_quantile($1, $2, 1, $3, 0.1, ARRAY[$4]::double precision[])).value $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, double precision, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, sample_percent double precision, quantiles double precision[] DEFAULT NULL::double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_quantile($1, $2, 1, TRUE, $3, $4) $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, sample_percent double precision, quantiles double precision[]) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, sample_percent double precision, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, 1, TRUE, $3, ARRAY[$4]::double precision[])).value $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, sample_percent double precision, quantile double precision) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, integer, double precision, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, nband integer, sample_percent double precision, quantiles double precision[] DEFAULT NULL::double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_quantile($1, $2, $3, TRUE, $4, $5) $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, nband integer, sample_percent double precision, quantiles double precision[]) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, integer, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, nband integer, sample_percent double precision, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, $3, TRUE, $4, ARRAY[$5]::double precision[])).value $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, nband integer, sample_percent double precision, quantile double precision) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, integer, boolean, double precision, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, sample_percent double precision DEFAULT 0.1, quantiles double precision[] DEFAULT NULL::double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_quantile($1, $2, $3, $4, $5, $6) $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, quantiles double precision[]) OWNER TO postgres;

--
-- Name: st_approxquantile(text, text, integer, boolean, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxquantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, $3, $4, $5, ARRAY[$6]::double precision[])).value $_$;


ALTER FUNCTION public.st_approxquantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision, quantile double precision) OWNER TO postgres;

--
-- Name: st_approxsummarystats(text, text, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxsummarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, 1, $3, 0.1) $_$;


ALTER FUNCTION public.st_approxsummarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean) OWNER TO postgres;

--
-- Name: st_approxsummarystats(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxsummarystats(rastertable text, rastercolumn text, sample_percent double precision) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, 1, TRUE, $3) $_$;


ALTER FUNCTION public.st_approxsummarystats(rastertable text, rastercolumn text, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxsummarystats(text, text, integer, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxsummarystats(rastertable text, rastercolumn text, nband integer, sample_percent double precision) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, $3, TRUE, $4) $_$;


ALTER FUNCTION public.st_approxsummarystats(rastertable text, rastercolumn text, nband integer, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_approxsummarystats(text, text, integer, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_approxsummarystats(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, sample_percent double precision DEFAULT 0.1) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, $3, $4, $5) $_$;


ALTER FUNCTION public.st_approxsummarystats(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, sample_percent double precision) OWNER TO postgres;

--
-- Name: st_area(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_area(text) RETURNS double precision
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_Area($1::geometry);  $_$;


ALTER FUNCTION public.st_area(text) OWNER TO postgres;

--
-- Name: st_asewkt(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_asewkt(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_AsEWKT($1::geometry);  $_$;


ALTER FUNCTION public.st_asewkt(text) OWNER TO postgres;

--
-- Name: st_asgeojson(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_asgeojson(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT _ST_AsGeoJson(1, $1::geometry,15,0);  $_$;


ALTER FUNCTION public.st_asgeojson(text) OWNER TO postgres;

--
-- Name: st_asgml(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_asgml(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT _ST_AsGML(2,$1::geometry,15,0, NULL);  $_$;


ALTER FUNCTION public.st_asgml(text) OWNER TO postgres;

--
-- Name: st_askml(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_askml(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT _ST_AsKML(2, $1::geometry, 15, null);  $_$;


ALTER FUNCTION public.st_askml(text) OWNER TO postgres;

--
-- Name: st_assvg(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_assvg(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_AsSVG($1::geometry,0,15);  $_$;


ALTER FUNCTION public.st_assvg(text) OWNER TO postgres;

--
-- Name: st_astext(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_astext(text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_AsText($1::geometry);  $_$;


ALTER FUNCTION public.st_astext(text) OWNER TO postgres;

--
-- Name: st_count(text, text, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_count(rastertable text, rastercolumn text, exclude_nodata_value boolean) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, 1, $3, 1) $_$;


ALTER FUNCTION public.st_count(rastertable text, rastercolumn text, exclude_nodata_value boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_count(rastertable text, rastercolumn text, exclude_nodata_value boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_count(rastertable text, rastercolumn text, exclude_nodata_value boolean) IS 'args: rastertable, rastercolumn, exclude_nodata_value - Returns the number of pixels in a given band of a raster or raster coverage. If no band is specified defaults to band 1. If exclude_nodata_value is set to true, will only count pixels that are not equal to the nodata value.';


--
-- Name: st_count(text, text, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_count(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_count($1, $2, $3, $4, 1) $_$;


ALTER FUNCTION public.st_count(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_count(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_count(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean) IS 'args: rastertable, rastercolumn, nband=1, exclude_nodata_value=true - Returns the number of pixels in a given band of a raster or raster coverage. If no band is specified defaults to band 1. If exclude_nodata_value is set to true, will only count pixels that are not equal to the nodata value.';


--
-- Name: st_coveredby(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_coveredby(text, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT ST_CoveredBy($1::geometry, $2::geometry);  $_$;


ALTER FUNCTION public.st_coveredby(text, text) OWNER TO postgres;

--
-- Name: st_covers(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_covers(text, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT ST_Covers($1::geometry, $2::geometry);  $_$;


ALTER FUNCTION public.st_covers(text, text) OWNER TO postgres;

--
-- Name: st_distance(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_distance(text, text) RETURNS double precision
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_Distance($1::geometry, $2::geometry);  $_$;


ALTER FUNCTION public.st_distance(text, text) OWNER TO postgres;

--
-- Name: st_distinct4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_distinct4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT COUNT(DISTINCT unnest)::float FROM unnest($1) $_$;


ALTER FUNCTION public.st_distinct4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_distinct4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_distinct4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the number of unique pixel values in a neighborhood.';


--
-- Name: st_dwithin(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_dwithin(text, text, double precision) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT ST_DWithin($1::geometry, $2::geometry, $3);  $_$;


ALTER FUNCTION public.st_dwithin(text, text, double precision) OWNER TO postgres;

--
-- Name: st_histogram(text, text, integer, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, "right" boolean) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, TRUE, 1, $4, NULL, $5) $_$;


ALTER FUNCTION public.st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, "right" boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, "right" boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, "right" boolean) IS 'args: rastertable, rastercolumn, nband, bins, right - Returns a set of histogram summarizing a raster or raster coverage data distribution separate bin ranges. Number of bins are autocomputed if not specified.';


--
-- Name: st_histogram(text, text, integer, boolean, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, "right" boolean) RETURNS SETOF histogram
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_histogram($1, $2, $3, $4, 1, $5, NULL, $6) $_$;


ALTER FUNCTION public.st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, "right" boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, "right" boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, "right" boolean) IS 'args: rastertable, rastercolumn, nband, exclude_nodata_value, bins, right - Returns a set of histogram summarizing a raster or raster coverage data distribution separate bin ranges. Number of bins are autocomputed if not specified.';


--
-- Name: st_histogram(text, text, integer, integer, double precision[], boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, width double precision[] DEFAULT NULL::double precision[], "right" boolean DEFAULT false) RETURNS SETOF histogram
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_histogram($1, $2, $3, TRUE, 1, $4, $5, $6) $_$;


ALTER FUNCTION public.st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, width double precision[], "right" boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, width double precision[], "right" boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, bins integer, width double precision[], "right" boolean) IS 'args: rastertable, rastercolumn, nband=1, bins, width=NULL, right=false - Returns a set of histogram summarizing a raster or raster coverage data distribution separate bin ranges. Number of bins are autocomputed if not specified.';


--
-- Name: st_histogram(text, text, integer, boolean, integer, double precision[], boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, bins integer DEFAULT 0, width double precision[] DEFAULT NULL::double precision[], "right" boolean DEFAULT false) RETURNS SETOF histogram
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_histogram($1, $2, $3, $4, 1, $5, $6, $7) $_$;


ALTER FUNCTION public.st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, width double precision[], "right" boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, width double precision[], "right" boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_histogram(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, bins integer, width double precision[], "right" boolean) IS 'args: rastertable, rastercolumn, nband=1, exclude_nodata_value=true, bins=autocomputed, width=NULL, right=false - Returns a set of histogram summarizing a raster or raster coverage data distribution separate bin ranges. Number of bins are autocomputed if not specified.';


--
-- Name: st_intersects(text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_intersects(text, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT ST_Intersects($1::geometry, $2::geometry);  $_$;


ALTER FUNCTION public.st_intersects(text, text) OWNER TO postgres;

--
-- Name: st_length(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_length(text) RETURNS double precision
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT ST_Length($1::geometry);  $_$;


ALTER FUNCTION public.st_length(text) OWNER TO postgres;

--
-- Name: st_max4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_max4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        _matrix float[][];
        max float;
    BEGIN
        _matrix := matrix;
        max := '-Infinity'::float;
        FOR x in array_lower(_matrix, 1)..array_upper(_matrix, 1) LOOP
            FOR y in array_lower(_matrix, 2)..array_upper(_matrix, 2) LOOP
                IF _matrix[x][y] IS NULL THEN
                    IF NOT nodatamode = 'ignore' THEN
                        _matrix[x][y] := nodatamode::float;
                    END IF;
                END IF;
                IF max < _matrix[x][y] THEN
                    max := _matrix[x][y];
                END IF;
            END LOOP;
        END LOOP;
        RETURN max;
    END;
    $$;


ALTER FUNCTION public.st_max4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_max4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_max4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the maximum pixel value in a neighborhood.';


--
-- Name: st_mean4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_mean4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        _matrix float[][];
        sum float;
        count float;
    BEGIN
        _matrix := matrix;
        sum := 0;
        count := 0;
        FOR x in array_lower(matrix, 1)..array_upper(matrix, 1) LOOP
            FOR y in array_lower(matrix, 2)..array_upper(matrix, 2) LOOP
                IF _matrix[x][y] IS NULL THEN
                    IF nodatamode = 'ignore' THEN
                        _matrix[x][y] := 0;
                    ELSE
                        _matrix[x][y] := nodatamode::float;
                        count := count + 1;
                    END IF;
                ELSE
                    count := count + 1;
                END IF;
                sum := sum + _matrix[x][y];
            END LOOP;
        END LOOP;
        IF count = 0 THEN
            RETURN NULL;
        END IF;
        RETURN sum / count;
    END;
    $$;


ALTER FUNCTION public.st_mean4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_mean4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_mean4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the mean pixel value in a neighborhood.';


--
-- Name: st_min4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_min4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        _matrix float[][];
        min float;
    BEGIN
        _matrix := matrix;
        min := 'Infinity'::float;
        FOR x in array_lower(_matrix, 1)..array_upper(_matrix, 1) LOOP
            FOR y in array_lower(_matrix, 2)..array_upper(_matrix, 2) LOOP
                IF _matrix[x][y] IS NULL THEN
                    IF NOT nodatamode = 'ignore' THEN
                        _matrix[x][y] := nodatamode::float;
                    END IF;
                END IF;
                IF min > _matrix[x][y] THEN
                    min := _matrix[x][y];
                END IF;
            END LOOP;
        END LOOP;
        RETURN min;
    END;
    $$;


ALTER FUNCTION public.st_min4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_min4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_min4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the minimum pixel value in a neighborhood.';


--
-- Name: st_quantile(text, text, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, quantiles double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_quantile($1, $2, 1, TRUE, 1, $3) $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, quantiles double precision[]) OWNER TO postgres;

--
-- Name: st_quantile(text, text, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, 1, TRUE, 1, ARRAY[$3]::double precision[])).value $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, quantile double precision) OWNER TO postgres;

--
-- Name: st_quantile(text, text, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision DEFAULT NULL::double precision) RETURNS double precision
    LANGUAGE sql STABLE
    AS $_$ SELECT (_st_quantile($1, $2, 1, $3, 1, ARRAY[$4]::double precision[])).value $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, exclude_nodata_value boolean, quantile double precision) OWNER TO postgres;

--
-- Name: st_quantile(text, text, integer, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, quantiles double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_quantile($1, $2, $3, TRUE, 1, $4) $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, nband integer, quantiles double precision[]) OWNER TO postgres;

--
-- Name: FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, quantiles double precision[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, quantiles double precision[]) IS 'args: rastertable, rastercolumn, nband, quantiles - Compute quantiles for a raster or raster table coverage in the context of the sample or population. Thus, a value could be examined to be at the rasters 25%, 50%, 75% percentile.';


--
-- Name: st_quantile(text, text, integer, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, $3, TRUE, 1, ARRAY[$4]::double precision[])).value $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, nband integer, quantile double precision) OWNER TO postgres;

--
-- Name: st_quantile(text, text, integer, boolean, double precision[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, quantiles double precision[] DEFAULT NULL::double precision[]) RETURNS SETOF quantile
    LANGUAGE sql STABLE
    AS $_$ SELECT _st_quantile($1, $2, $3, $4, 1, $5) $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, quantiles double precision[]) OWNER TO postgres;

--
-- Name: FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, quantiles double precision[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, quantiles double precision[]) IS 'args: rastertable, rastercolumn, nband=1, exclude_nodata_value=true, quantiles=NULL - Compute quantiles for a raster or raster table coverage in the context of the sample or population. Thus, a value could be examined to be at the rasters 25%, 50%, 75% percentile.';


--
-- Name: st_quantile(text, text, integer, boolean, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_quantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, quantile double precision) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_quantile($1, $2, $3, $4, 1, ARRAY[$5]::double precision[])).value $_$;


ALTER FUNCTION public.st_quantile(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, quantile double precision) OWNER TO postgres;

--
-- Name: st_range4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_range4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        _matrix float[][];
        min float;
        max float;
    BEGIN
        _matrix := matrix;
        min := 'Infinity'::float;
        max := '-Infinity'::float;
        FOR x in array_lower(matrix, 1)..array_upper(matrix, 1) LOOP
            FOR y in array_lower(matrix, 2)..array_upper(matrix, 2) LOOP
                IF _matrix[x][y] IS NULL THEN
                    IF NOT nodatamode = 'ignore' THEN
                        _matrix[x][y] := nodatamode::float;
                    END IF;
                END IF;
                IF min > _matrix[x][y] THEN
                    min = _matrix[x][y];
                END IF;
                IF max < _matrix[x][y] THEN
                    max = _matrix[x][y];
                END IF;
            END LOOP;
        END LOOP;
        IF max = '-Infinity'::float OR min = 'Infinity'::float THEN
            RETURN NULL;
        END IF;
        RETURN max - min;
    END;
    $$;


ALTER FUNCTION public.st_range4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_range4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_range4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the range of pixel values in a neighborhood.';


--
-- Name: st_samealignment(double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_samealignment(ulx1 double precision, uly1 double precision, scalex1 double precision, scaley1 double precision, skewx1 double precision, skewy1 double precision, ulx2 double precision, uly2 double precision, scalex2 double precision, scaley2 double precision, skewx2 double precision, skewy2 double precision) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$ SELECT st_samealignment(st_makeemptyraster(1, 1, $1, $2, $3, $4, $5, $6), st_makeemptyraster(1, 1, $7, $8, $9, $10, $11, $12)) $_$;


ALTER FUNCTION public.st_samealignment(ulx1 double precision, uly1 double precision, scalex1 double precision, scaley1 double precision, skewx1 double precision, skewy1 double precision, ulx2 double precision, uly2 double precision, scalex2 double precision, scaley2 double precision, skewx2 double precision, skewy2 double precision) OWNER TO postgres;

--
-- Name: FUNCTION st_samealignment(ulx1 double precision, uly1 double precision, scalex1 double precision, scaley1 double precision, skewx1 double precision, skewy1 double precision, ulx2 double precision, uly2 double precision, scalex2 double precision, scaley2 double precision, skewx2 double precision, skewy2 double precision); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_samealignment(ulx1 double precision, uly1 double precision, scalex1 double precision, scaley1 double precision, skewx1 double precision, skewy1 double precision, ulx2 double precision, uly2 double precision, scalex2 double precision, scaley2 double precision, skewx2 double precision, skewy2 double precision) IS 'args: ulx1, uly1, scalex1, scaley1, skewx1, skewy1, ulx2, uly2, scalex2, scaley2, skewx2, skewy2 - Returns true if rasters have same skew, scale, spatial ref and false if they dont with notice detailing issue.';


--
-- Name: st_stddev4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_stddev4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE sql IMMUTABLE
    AS $_$ SELECT stddev(unnest) FROM unnest($1) $_$;


ALTER FUNCTION public.st_stddev4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_stddev4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_stddev4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the standard deviation of pixel values in a neighborhood.';


--
-- Name: st_sum4ma(double precision[], text, text[]); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_sum4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) RETURNS double precision
    LANGUAGE plpgsql IMMUTABLE
    AS $$
    DECLARE
        _matrix float[][];
        sum float;
    BEGIN
        _matrix := matrix;
        sum := 0;
        FOR x in array_lower(matrix, 1)..array_upper(matrix, 1) LOOP
            FOR y in array_lower(matrix, 2)..array_upper(matrix, 2) LOOP
                IF _matrix[x][y] IS NULL THEN
                    IF nodatamode = 'ignore' THEN
                        _matrix[x][y] := 0;
                    ELSE
                        _matrix[x][y] := nodatamode::float;
                    END IF;
                END IF;
                sum := sum + _matrix[x][y];
            END LOOP;
        END LOOP;
        RETURN sum;
    END;
    $$;


ALTER FUNCTION public.st_sum4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) OWNER TO postgres;

--
-- Name: FUNCTION st_sum4ma(matrix double precision[], nodatamode text, VARIADIC args text[]); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_sum4ma(matrix double precision[], nodatamode text, VARIADIC args text[]) IS 'args: matrix, nodatamode, VARIADIC args - Raster processing function that calculates the sum of all pixel values in a neighborhood.';


--
-- Name: st_summarystats(text, text, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_summarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, 1, $3, 1) $_$;


ALTER FUNCTION public.st_summarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_summarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_summarystats(rastertable text, rastercolumn text, exclude_nodata_value boolean) IS 'args: rastertable, rastercolumn, exclude_nodata_value - Returns summary stats consisting of count,sum,mean,stddev,min,max for a given raster band of a raster or raster coverage. Band 1 is assumed is no band is specified.';


--
-- Name: st_summarystats(text, text, integer, boolean); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_summarystats(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true) RETURNS summarystats
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT _st_summarystats($1, $2, $3, $4, 1) $_$;


ALTER FUNCTION public.st_summarystats(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean) OWNER TO postgres;

--
-- Name: FUNCTION st_summarystats(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_summarystats(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean) IS 'args: rastertable, rastercolumn, nband=1, exclude_nodata_value=true - Returns summary stats consisting of count,sum,mean,stddev,min,max for a given raster band of a raster or raster coverage. Band 1 is assumed is no band is specified.';


--
-- Name: st_valuecount(text, text, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT count integer) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, count FROM _st_valuecount($1, $2, 1, TRUE, $3, $4) $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) IS 'args: rastertable, rastercolumn, searchvalues, roundto=0, OUT value, OUT count - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuecount(text, text, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, 1, TRUE, ARRAY[$3]::double precision[], $4)).count $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision) IS 'args: rastertable, rastercolumn, searchvalue, roundto=0 - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuecount(text, text, integer, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT count integer) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, count FROM _st_valuecount($1, $2, $3, TRUE, $4, $5) $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) IS 'args: rastertable, rastercolumn, nband, searchvalues, roundto=0, OUT value, OUT count - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuecount(text, text, integer, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, $3, TRUE, ARRAY[$4]::double precision[], $5)).count $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision) IS 'args: rastertable, rastercolumn, nband, searchvalue, roundto=0 - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuecount(text, text, integer, boolean, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, searchvalues double precision[] DEFAULT NULL::double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT count integer) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, count FROM _st_valuecount($1, $2, $3, $4, $5, $6) $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalues double precision[], roundto double precision, OUT value double precision, OUT count integer) IS 'args: rastertable, rastercolumn, nband=1, exclude_nodata_value=true, searchvalues=NULL, roundto=0, OUT value, OUT count - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuecount(text, text, integer, boolean, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS integer
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, $3, $4, ARRAY[$5]::double precision[], $6)).count $_$;


ALTER FUNCTION public.st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision); Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON FUNCTION st_valuecount(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision) IS 'args: rastertable, rastercolumn, nband, exclude_nodata_value, searchvalue, roundto=0 - Returns a set of records containing a pixel band value and count of the number of pixels in a given band of a raster (or a raster coverage) that have a given set of values. If no band is specified defaults to band 1. By default nodata value pixels are not counted. and all other values in the pixel are output and pixel band values are rounded to the nearest integer.';


--
-- Name: st_valuepercent(text, text, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT percent double precision) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, percent FROM _st_valuecount($1, $2, 1, TRUE, $3, $4) $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, searchvalues double precision[], roundto double precision, OUT value double precision, OUT percent double precision) OWNER TO postgres;

--
-- Name: st_valuepercent(text, text, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, 1, TRUE, ARRAY[$3]::double precision[], $4)).percent $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: st_valuepercent(text, text, integer, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT percent double precision) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, percent FROM _st_valuecount($1, $2, $3, TRUE, $4, $5) $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, nband integer, searchvalues double precision[], roundto double precision, OUT value double precision, OUT percent double precision) OWNER TO postgres;

--
-- Name: st_valuepercent(text, text, integer, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, $3, TRUE, ARRAY[$4]::double precision[], $5)).percent $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, nband integer, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: st_valuepercent(text, text, integer, boolean, double precision[], double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, nband integer DEFAULT 1, exclude_nodata_value boolean DEFAULT true, searchvalues double precision[] DEFAULT NULL::double precision[], roundto double precision DEFAULT 0, OUT value double precision, OUT percent double precision) RETURNS SETOF record
    LANGUAGE sql STABLE
    AS $_$ SELECT value, percent FROM _st_valuecount($1, $2, $3, $4, $5, $6) $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalues double precision[], roundto double precision, OUT value double precision, OUT percent double precision) OWNER TO postgres;

--
-- Name: st_valuepercent(text, text, integer, boolean, double precision, double precision); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION st_valuepercent(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision DEFAULT 0) RETURNS double precision
    LANGUAGE sql STABLE STRICT
    AS $_$ SELECT (_st_valuecount($1, $2, $3, $4, ARRAY[$5]::double precision[], $6)).percent $_$;


ALTER FUNCTION public.st_valuepercent(rastertable text, rastercolumn text, nband integer, exclude_nodata_value boolean, searchvalue double precision, roundto double precision) OWNER TO postgres;

--
-- Name: unlockrows(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION unlockrows(text) RETURNS integer
    LANGUAGE plpgsql STRICT
    AS $_$ 
DECLARE
	ret int;
BEGIN

	IF NOT LongTransactionsEnabled() THEN
		RAISE EXCEPTION 'Long transaction support disabled, use EnableLongTransaction() to enable.';
	END IF;

	EXECUTE 'DELETE FROM authorization_table where authid = ' ||
		quote_literal($1);

	GET DIAGNOSTICS ret = ROW_COUNT;

	RETURN ret;
END;
$_$;


ALTER FUNCTION public.unlockrows(text) OWNER TO postgres;

--
-- Name: updategeometrysrid(character varying, character varying, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION updategeometrysrid(character varying, character varying, integer) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $_$
DECLARE
	ret  text;
BEGIN
	SELECT UpdateGeometrySRID('','',$1,$2,$3) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.updategeometrysrid(character varying, character varying, integer) OWNER TO postgres;

--
-- Name: updategeometrysrid(character varying, character varying, character varying, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION updategeometrysrid(character varying, character varying, character varying, integer) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $_$
DECLARE
	ret  text;
BEGIN
	SELECT UpdateGeometrySRID('',$1,$2,$3,$4) into ret;
	RETURN ret;
END;
$_$;


ALTER FUNCTION public.updategeometrysrid(character varying, character varying, character varying, integer) OWNER TO postgres;

--
-- Name: updategeometrysrid(character varying, character varying, character varying, character varying, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION updategeometrysrid(catalogn_name character varying, schema_name character varying, table_name character varying, column_name character varying, new_srid_in integer) RETURNS text
    LANGUAGE plpgsql STRICT
    AS $$
DECLARE
	myrec RECORD;
	okay boolean;
	cname varchar;
	real_schema name;
	unknown_srid integer;
	new_srid integer := new_srid_in;

BEGIN


	-- Find, check or fix schema_name
	IF ( schema_name != '' ) THEN
		okay = false;

		FOR myrec IN SELECT nspname FROM pg_namespace WHERE text(nspname) = schema_name LOOP
			okay := true;
		END LOOP;

		IF ( okay <> true ) THEN
			RAISE EXCEPTION 'Invalid schema name';
		ELSE
			real_schema = schema_name;
		END IF;
	ELSE
		SELECT INTO real_schema current_schema()::text;
	END IF;

	-- Ensure that column_name is in geometry_columns
	okay = false;
	FOR myrec IN SELECT type, coord_dimension FROM geometry_columns WHERE f_table_schema = text(real_schema) and f_table_name = table_name and f_geometry_column = column_name LOOP
		okay := true;
	END LOOP;
	IF (NOT okay) THEN
		RAISE EXCEPTION 'column not found in geometry_columns table';
		RETURN false;
	END IF;

	-- Ensure that new_srid is valid
	IF ( new_srid > 0 ) THEN
		IF ( SELECT count(*) = 0 from spatial_ref_sys where srid = new_srid ) THEN
			RAISE EXCEPTION 'invalid SRID: % not found in spatial_ref_sys', new_srid;
			RETURN false;
		END IF;
	ELSE
		unknown_srid := ST_SRID('POINT EMPTY'::geometry);
		IF ( new_srid != unknown_srid ) THEN
			new_srid := unknown_srid;
			RAISE NOTICE 'SRID value % converted to the officially unknown SRID value %', new_srid_in, new_srid;
		END IF;
	END IF;

	IF postgis_constraint_srid(schema_name, table_name, column_name) IS NOT NULL THEN 
	-- srid was enforced with constraints before, keep it that way.
        -- Make up constraint name
        cname = 'enforce_srid_'  || column_name;
    
        -- Drop enforce_srid constraint
        EXECUTE 'ALTER TABLE ' || quote_ident(real_schema) ||
            '.' || quote_ident(table_name) ||
            ' DROP constraint ' || quote_ident(cname);
    
        -- Update geometries SRID
        EXECUTE 'UPDATE ' || quote_ident(real_schema) ||
            '.' || quote_ident(table_name) ||
            ' SET ' || quote_ident(column_name) ||
            ' = ST_SetSRID(' || quote_ident(column_name) ||
            ', ' || new_srid::text || ')';
            
        -- Reset enforce_srid constraint
        EXECUTE 'ALTER TABLE ' || quote_ident(real_schema) ||
            '.' || quote_ident(table_name) ||
            ' ADD constraint ' || quote_ident(cname) ||
            ' CHECK (st_srid(' || quote_ident(column_name) ||
            ') = ' || new_srid::text || ')';
    ELSE 
        -- We will use typmod to enforce if no srid constraints
        -- We are using postgis_type_name to lookup the new name 
        -- (in case Paul changes his mind and flips geometry_columns to return old upper case name) 
        EXECUTE 'ALTER TABLE ' || quote_ident(real_schema) || '.' || quote_ident(table_name) || 
        ' ALTER COLUMN ' || quote_ident(column_name) || ' TYPE  geometry(' || postgis_type_name(myrec.type, myrec.coord_dimension, true) || ', ' || new_srid::text || ') USING ST_SetSRID(' || quote_ident(column_name) || ',' || new_srid::text || ');' ;
    END IF;

	RETURN real_schema || '.' || table_name || '.' || column_name ||' SRID changed to ' || new_srid::text;

END;
$$;


ALTER FUNCTION public.updategeometrysrid(catalogn_name character varying, schema_name character varying, table_name character varying, column_name character varying, new_srid_in integer) OWNER TO postgres;

--
-- Name: btree_geography_ops; Type: OPERATOR FAMILY; Schema: public; Owner: postgres
--

CREATE OPERATOR FAMILY btree_geography_ops USING btree;


ALTER OPERATOR FAMILY public.btree_geography_ops USING btree OWNER TO postgres;

--
-- Name: btree_geometry_ops; Type: OPERATOR FAMILY; Schema: public; Owner: postgres
--

CREATE OPERATOR FAMILY btree_geometry_ops USING btree;


ALTER OPERATOR FAMILY public.btree_geometry_ops USING btree OWNER TO postgres;

--
-- Name: gist_geography_ops; Type: OPERATOR FAMILY; Schema: public; Owner: postgres
--

CREATE OPERATOR FAMILY gist_geography_ops USING gist;


ALTER OPERATOR FAMILY public.gist_geography_ops USING gist OWNER TO postgres;

--
-- Name: gist_geometry_ops_2d; Type: OPERATOR FAMILY; Schema: public; Owner: postgres
--

CREATE OPERATOR FAMILY gist_geometry_ops_2d USING gist;


ALTER OPERATOR FAMILY public.gist_geometry_ops_2d USING gist OWNER TO postgres;

--
-- Name: gist_geometry_ops_nd; Type: OPERATOR FAMILY; Schema: public; Owner: postgres
--

CREATE OPERATOR FAMILY gist_geometry_ops_nd USING gist;


ALTER OPERATOR FAMILY public.gist_geometry_ops_nd USING gist OWNER TO postgres;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: a; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE a (
    min numeric,
    department_id numeric(4,0)
);


ALTER TABLE public.a OWNER TO postgres;

--
-- Name: countries; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE countries (
    country_id character varying(2) NOT NULL,
    country_name character varying(40) DEFAULT NULL::character varying,
    region_id numeric(10,0) DEFAULT NULL::numeric
);


ALTER TABLE public.countries OWNER TO postgres;

--
-- Name: countries1; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE countries1 (
    country_id character varying(3),
    country_name character varying(45),
    region_id numeric(10,0)
);


ALTER TABLE public.countries1 OWNER TO postgres;

--
-- Name: countries123; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE countries123 (
    country_id character varying(3),
    country_name character varying(45),
    region_id numeric(10,0)
);


ALTER TABLE public.countries123 OWNER TO postgres;

--
-- Name: countries2; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE countries2 (
    country_id integer,
    country_name character varying(15),
    country_region character varying(15)
);


ALTER TABLE public.countries2 OWNER TO postgres;

--
-- Name: country_new; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE country_new (
    country_id character varying(2),
    country_name character varying(40),
    region_id numeric(10,0)
);


ALTER TABLE public.country_new OWNER TO postgres;

--
-- Name: country_new123; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE country_new123 (
    country_id character varying(3),
    country_name character varying(45),
    region_id numeric(10,0)
);


ALTER TABLE public.country_new123 OWNER TO postgres;

--
-- Name: departments; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE departments (
    department_id numeric(4,0) NOT NULL,
    department_name character varying(30) NOT NULL,
    manager_id numeric(6,0) DEFAULT NULL::numeric,
    location_id numeric(4,0) DEFAULT NULL::numeric
);


ALTER TABLE public.departments OWNER TO postgres;

--
-- Name: emp; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE emp (
    emp_id integer,
    emp_name character(10),
    emp_city character(10)
);


ALTER TABLE public.emp OWNER TO postgres;

--
-- Name: emp1; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE emp1 (
    emp_id integer,
    emp_name character(20),
    emp_city character(10)
);


ALTER TABLE public.emp1 OWNER TO postgres;

--
-- Name: employees; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE employees (
    employee_id numeric(6,0) DEFAULT (0)::numeric NOT NULL,
    first_name character varying(20) DEFAULT NULL::character varying,
    last_name character varying(25) NOT NULL,
    email character varying(25) NOT NULL,
    phone_number character varying(20) DEFAULT NULL::character varying,
    hire_date date NOT NULL,
    job_id character varying(10) NOT NULL,
    salary numeric(8,2) DEFAULT NULL::numeric,
    commission_pct numeric(2,2) DEFAULT NULL::numeric,
    manager_id numeric(6,0) DEFAULT NULL::numeric,
    department_id numeric(4,0) DEFAULT NULL::numeric
);


ALTER TABLE public.employees OWNER TO postgres;

--
-- Name: job_grades; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE job_grades (
    grade_level character(2) NOT NULL,
    lowest_sal integer,
    highest_sal integer
);


ALTER TABLE public.job_grades OWNER TO postgres;

--
-- Name: job_history; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE job_history (
    employee_id numeric(6,0) NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL,
    job_id character varying(10) NOT NULL,
    department_id numeric(4,0) DEFAULT NULL::numeric
);


ALTER TABLE public.job_history OWNER TO postgres;

--
-- Name: jobs; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE jobs (
    job_id character varying(10) DEFAULT ''::character varying NOT NULL,
    job_title character varying(35) NOT NULL,
    min_salary numeric(6,0) DEFAULT NULL::numeric,
    max_salary numeric(6,0) DEFAULT NULL::numeric
);


ALTER TABLE public.jobs OWNER TO postgres;

--
-- Name: locations; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE locations (
    location_id numeric(4,0) DEFAULT (0)::numeric NOT NULL,
    street_address character varying(40) DEFAULT NULL::character varying,
    postal_code character varying(12) DEFAULT NULL::character varying,
    city character varying(30) NOT NULL,
    state_province character varying(25) DEFAULT NULL::character varying,
    country_id character varying(2) DEFAULT NULL::character varying
);


ALTER TABLE public.locations OWNER TO postgres;

--
-- Name: max_sal; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW max_sal AS
 SELECT employees.department_id,
    max(employees.salary) AS max_salary
   FROM employees
  GROUP BY employees.department_id;


ALTER TABLE public.max_sal OWNER TO postgres;

--
-- Name: max_salaries; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW max_salaries AS
 SELECT employees.department_id,
    max(employees.salary) AS max_salary
   FROM employees
  GROUP BY employees.department_id;


ALTER TABLE public.max_salaries OWNER TO postgres;

--
-- Name: max_salary; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW max_salary AS
 SELECT employees.department_id,
    max(employees.salary) AS max
   FROM employees
  GROUP BY employees.department_id;


ALTER TABLE public.max_salary OWNER TO postgres;

--
-- Name: myresidents; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE myresidents (
    first_name character(10),
    last_name character(10),
    unit character(10),
    status character(15),
    market_rent numeric(10,2)
);


ALTER TABLE public.myresidents OWNER TO postgres;

--
-- Name: new_table; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE new_table (
    country_id character varying(3),
    country_name character varying(45),
    region_id numeric(10,0)
);


ALTER TABLE public.new_table OWNER TO postgres;

--
-- Name: number_employees; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE number_employees (
    department_id numeric(4,0),
    no_employees bigint
);


ALTER TABLE public.number_employees OWNER TO postgres;

--
-- Name: numberofemployees; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE numberofemployees (
    department_id numeric(4,0),
    no_employees bigint
);


ALTER TABLE public.numberofemployees OWNER TO postgres;

--
-- Name: persons; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE persons (
    p_id integer NOT NULL,
    lastname character varying(255) NOT NULL,
    firstname character varying(255),
    address character varying(255),
    city character varying(255) DEFAULT 'Sandnes'::character varying
);


ALTER TABLE public.persons OWNER TO postgres;

--
-- Name: personsnotnull; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE personsnotnull (
    p_id integer NOT NULL,
    lastname character varying(255) NOT NULL,
    firstname character varying(255),
    address character varying(255),
    city character varying(255)
);


ALTER TABLE public.personsnotnull OWNER TO postgres;

--
-- Name: raster_overviews; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW raster_overviews AS
 SELECT current_database() AS o_table_catalog,
    n.nspname AS o_table_schema,
    c.relname AS o_table_name,
    a.attname AS o_raster_column,
    current_database() AS r_table_catalog,
    (split_part(split_part(s.consrc, '''::name'::text, 1), ''''::text, 2))::name AS r_table_schema,
    (split_part(split_part(s.consrc, '''::name'::text, 2), ''''::text, 2))::name AS r_table_name,
    (split_part(split_part(s.consrc, '''::name'::text, 3), ''''::text, 2))::name AS r_raster_column,
    (btrim(split_part(s.consrc, ','::text, 2)))::integer AS overview_factor
   FROM pg_class c,
    pg_attribute a,
    pg_type t,
    pg_namespace n,
    pg_constraint s
  WHERE ((((((((((t.typname = 'raster'::name) AND (a.attisdropped = false)) AND (a.atttypid = t.oid)) AND (a.attrelid = c.oid)) AND (c.relnamespace = n.oid)) AND ((c.relkind = 'r'::"char") OR (c.relkind = 'v'::"char"))) AND (s.connamespace = n.oid)) AND (s.conrelid = c.oid)) AND (s.consrc ~~ '%_overview_constraint(%'::text)) AND (NOT pg_is_other_temp_schema(c.relnamespace)));


ALTER TABLE public.raster_overviews OWNER TO postgres;

--
-- Name: regions; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE regions (
    region_id numeric(10,0) NOT NULL,
    region_name character(25)
);


ALTER TABLE public.regions OWNER TO postgres;

--
-- Name: residents; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE residents (
    first_name character(10),
    last_name character(10),
    unit character(10),
    status character(15),
    market_rent numeric(10,2)
);


ALTER TABLE public.residents OWNER TO postgres;

--
-- Name: school; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE school (
    emp_name character varying(20),
    age integer,
    salary integer,
    class integer
);


ALTER TABLE public.school OWNER TO postgres;

--
-- Name: temp_1; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW temp_1 AS
 SELECT max(employees.salary) AS max
   FROM employees
  GROUP BY employees.department_id;


ALTER TABLE public.temp_1 OWNER TO postgres;

--
-- Name: temp_employee; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE temp_employee (
    employee_id numeric(6,0),
    first_name character varying(20),
    last_name character varying(25),
    email character varying(25),
    phone_number character varying(20),
    hire_date date,
    job_id character varying(10),
    salary numeric(8,2),
    commission_pct numeric(2,2),
    manager_id numeric(6,0),
    department_id numeric(4,0)
);


ALTER TABLE public.temp_employee OWNER TO postgres;

--
-- Name: temptable; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE temptable (
    employee_id numeric(6,0),
    first_name character varying(20),
    last_name character varying(25),
    email character varying(25),
    phone_number character varying(20),
    hire_date date,
    job_id character varying(10),
    salary numeric(8,2),
    commission_pct numeric(2,2),
    manager_id numeric(6,0),
    department_id numeric(4,0)
);


ALTER TABLE public.temptable OWNER TO postgres;

--
-- Name: test; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE test (
    name character varying(10)
);


ALTER TABLE public.test OWNER TO postgres;

--
-- Data for Name: a; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY a (min, department_id) FROM stdin;
10000.00	70
6000.00	20
6500.00	40
2500.00	30
4200.00	60
4400.00	10
6100.00	80
2100.00	50
17000.00	90
8300.00	110
6900.00	100
7000.00	0
\.


--
-- Data for Name: countries; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY countries (country_id, country_name, region_id) FROM stdin;
AR	Argentina	2
AU	Australia	3
BE	Belgium	1
BR	Brazil	2
CA	Canada	2
CH	Switzerland	1
CN	China	3
DE	Germany	1
DK	Denmark	1
EG	Egypt	4
FR	France	1
HK	HongKong	3
IL	Israel	4
IN	India	3
IT	Italy	1
JP	Japan	3
KW	Kuwait	4
MX	Mexico	2
NG	Nigeria	4
NL	Netherlands	1
SG	Singapore	3
UK	United Kingdom	1
US	United States of America	2
ZM	Zambia	4
ZW	Zimbabwe	4
12	Belgia	\N
RO	Romania	\N
BG	Romania	\N
\.


--
-- Data for Name: countries1; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY countries1 (country_id, country_name, region_id) FROM stdin;
C1	India	1002
C2	USA	\N
\N	UK	\N
C4	India	1001
C5	USA	1007
C6	UK	1003
\.


--
-- Data for Name: countries123; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY countries123 (country_id, country_name, region_id) FROM stdin;
\.


--
-- Data for Name: countries2; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY countries2 (country_id, country_name, country_region) FROM stdin;
12	Germany	123
\.


--
-- Data for Name: country_new; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY country_new (country_id, country_name, region_id) FROM stdin;
AR	Argentina	2
AU	Australia	3
BE	Belgium	1
BR	Brazil	2
CA	Canada	2
CH	Switzerland	1
CN	China	3
DE	Germany	1
DK	Denmark	1
EG	Egypt	4
FR	France	1
HK	HongKong	3
IL	Israel	4
IN	India	3
IT	Italy	1
JP	Japan	3
KW	Kuwait	4
MX	Mexico	2
NG	Nigeria	4
NL	Netherlands	1
SG	Singapore	3
UK	United Kingdom	1
US	United States of America	2
ZM	Zambia	4
ZW	Zimbabwe	4
12	Belgia	\N
\.


--
-- Data for Name: country_new123; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY country_new123 (country_id, country_name, region_id) FROM stdin;
C1	India	1002
C2	USA	\N
\N	UK	\N
\.


--
-- Data for Name: departments; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY departments (department_id, department_name, manager_id, location_id) FROM stdin;
10	Administration	200	1700
20	Marketing	201	1800
30	Purchasing	114	1700
40	Human Resources	203	2400
50	Shipping	121	1500
60	IT	103	1400
70	Public Relations	204	2700
80	Sales	145	2500
90	Executive	100	1700
100	Finance	108	1700
110	Accounting	205	1700
120	Treasury	0	1700
130	Corporate Tax	0	1700
140	Control And Credit	0	1700
150	Shareholder Services	0	1700
160	Benefits	0	1700
170	Manufacturing	0	1700
180	Construction	0	1700
190	Contracting	0	1700
200	Operations	0	1700
210	IT Support	0	1700
220	NOC	0	1700
230	IT Helpdesk	0	1700
240	Government Sales	0	1700
250	Retail Sales	0	1700
260	Recruiting	0	1700
270	Payroll	0	1700
\.


--
-- Data for Name: emp; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY emp (emp_id, emp_name, emp_city) FROM stdin;
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
101	abc       	pune      
\.


--
-- Data for Name: emp1; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY emp1 (emp_id, emp_name, emp_city) FROM stdin;
\.


--
-- Data for Name: employees; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY employees (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id) FROM stdin;
100	Steven	King	SKING	515.123.4567	1987-06-17	AD_PRES	24000.00	0.00	0	90
101	Neena	Kochhar	SKING	515.123.4568	1987-06-18	AD_VP	17000.00	0.00	100	90
102	Lex	De Haan	SKING	515.123.4569	1987-06-19	AD_VP	17000.00	0.00	100	90
103	Alexander	Hunold	SKING	590.423.4567	1987-06-20	IT_PROG	9000.00	0.00	102	60
104	Bruce	Ernst	SKING	590.423.4568	1987-06-21	IT_PROG	6000.00	0.00	103	60
105	David	Austin	SKING	590.423.4569	1987-06-22	IT_PROG	4800.00	0.00	103	60
106	Valli	Pataballa	SKING	590.423.4560	1987-06-23	IT_PROG	4800.00	0.00	103	60
107	Diana	Lorentz	SKING	590.423.5567	1987-06-24	IT_PROG	4200.00	0.00	103	60
114	Den	Raphaely	SKING	515.127.4561	1987-07-01	PU_MAN	11000.00	0.00	100	30
115	Alexander	Khoo	SKING	515.127.4562	1987-07-02	PU_CLERK	3100.00	0.00	114	30
116	Shelli	Baida	SKING	515.127.4563	1987-07-03	PU_CLERK	2900.00	0.00	114	30
117	Sigal	Tobias	SKING	515.127.4564	1987-07-04	PU_CLERK	2800.00	0.00	114	30
108	Nancy	Greenberg	SKING	515.999.4569	1987-06-25	FI_MGR	12000.00	0.00	101	100
109	Daniel	Faviet	SKING	515.999.4169	1987-06-26	FI_ACCOUNT	9000.00	0.00	108	100
110	John	Chen	SKING	515.999.4269	1987-06-27	FI_ACCOUNT	8200.00	0.00	108	100
111	Ismael	Sciarra	SKING	515.999.4369	1987-06-28	FI_ACCOUNT	7700.00	0.00	108	100
112	Jose Manuel	Urman	SKING	515.999.4469	1987-06-29	FI_ACCOUNT	7800.00	0.00	108	100
113	Luis	Popp	SKING	515.999.4567	1987-06-30	FI_ACCOUNT	6900.00	0.00	108	100
133	Jason	Mallin	SKING	650.127.1934	1987-07-20	ST_CLERK	3300.00	0.00	122	50
134	Michael	Rogers	SKING	650.127.1834	1987-07-21	ST_CLERK	2900.00	0.00	122	50
135	Ki	Gee	SKING	650.127.1734	1987-07-22	ST_CLERK	2400.00	0.00	122	50
136	Hazel	Philtanker	SKING	650.127.1634	1987-07-23	ST_CLERK	2200.00	0.00	122	50
137	Renske	Ladwig	SKING	650.121.1234	1987-07-24	ST_CLERK	3600.00	0.00	123	50
138	Stephen	Stiles	SKING	650.121.2034	1987-07-25	ST_CLERK	3200.00	0.00	123	50
139	John	Seo	SKING	650.121.2019	1987-07-26	ST_CLERK	2700.00	0.00	123	50
140	Joshua	Patel	SKING	650.121.1834	1987-07-27	ST_CLERK	2500.00	0.00	123	50
129	Laura	Bissot	SKING	650.999.5234	1987-07-16	ST_CLERK	3300.00	0.00	121	50
130	Mozhe	Atkinson	SKING	650.999.6234	1987-07-17	ST_CLERK	2800.00	0.00	121	50
131	James	Marlow	SKING	650.999.7234	1987-07-18	ST_CLERK	2500.00	0.00	121	50
132	TJ	Olson	SKING	650.999.8234	1987-07-19	ST_CLERK	2100.00	0.00	121	50
141	Trenna	Rajs	SKING	650.121.8009	1987-07-28	ST_CLERK	3500.00	0.00	124	50
142	Curtis	Davies	SKING	650.121.2994	1987-07-29	ST_CLERK	3100.00	0.00	124	50
143	Randall	Matos	SKING	650.121.2874	1987-07-30	ST_CLERK	2600.00	0.00	124	50
144	Peter	Vargas	SKING	650.121.2004	1987-07-31	ST_CLERK	2500.00	0.00	124	50
145	John	Russell	SKING	011.44.1344.429268	1987-08-01	SA_MAN	14000.00	0.40	100	80
146	Karen	Partners	SKING	011.44.1344.467268	1987-08-02	SA_MAN	13500.00	0.30	100	80
147	Alberto	Errazuriz	SKING	011.44.1344.429278	1987-08-03	SA_MAN	12000.00	0.30	100	80
148	Gerald	Cambrault	SKING	011.44.1344.619268	1987-08-04	SA_MAN	11000.00	0.30	100	80
149	Eleni	Zlotkey	SKING	011.44.1344.429018	1987-08-05	SA_MAN	10500.00	0.20	100	80
150	Peter	Tucker	SKING	011.44.1344.129268	1987-08-06	SA_REP	10000.00	0.30	145	80
118	Guy	Himuro	SKING	515.127.4565	1987-07-05	PU_CLERK	2600.00	0.00	114	30
119	Karen	Colmenares	SKING	515.127.4566	1987-07-06	PU_CLERK	2500.00	0.00	114	30
120	Matthew	Weiss	SKING	650.123.1234	1987-07-07	ST_MAN	8000.00	0.00	100	50
121	Adam	Fripp	SKING	650.123.2234	1987-07-08	ST_MAN	8200.00	0.00	100	50
122	Payam	Kaufling	SKING	650.123.3234	1987-07-09	ST_MAN	7900.00	0.00	100	50
123	Shanta	Vollman	SKING	650.123.4234	1987-07-10	ST_MAN	6500.00	0.00	100	50
124	Kevin	Mourgos	SKING	650.123.5234	1987-07-11	ST_MAN	5800.00	0.00	100	50
151	David	Bernstein	SKING	011.44.1344.345268	1987-08-07	SA_REP	9500.00	0.25	145	80
152	Peter	Hall	SKING	011.44.1344.478968	1987-08-08	SA_REP	9000.00	0.25	145	80
153	Christopher	Olsen	SKING	011.44.1344.498718	1987-08-09	SA_REP	8000.00	0.20	145	80
154	Nanette	Cambrault	SKING	011.44.1344.987668	1987-08-10	SA_REP	7500.00	0.20	145	80
155	Oliver	Tuvault	SKING	011.44.1344.486508	1987-08-11	SA_REP	7000.00	0.15	145	80
156	Janette	King	SKING	011.44.1345.429268	1987-08-12	SA_REP	10000.00	0.35	146	80
157	Patrick	Sully	SKING	011.44.1345.929268	1987-08-13	SA_REP	9500.00	0.35	146	80
158	Allan	McEwen	SKING	011.44.1345.829268	1987-08-14	SA_REP	9000.00	0.35	146	80
159	Lindsey	Smith	SKING	011.44.1345.729268	1987-08-15	SA_REP	8000.00	0.30	146	80
125	Julia	Nayer	SKING	650.999.1214	1987-07-12	ST_CLERK	3200.00	0.00	120	50
126	Irene	Mikkilineni	SKING	650.999.1224	1987-07-13	ST_CLERK	2700.00	0.00	120	50
127	James	Landry	SKING	650.999.1334	1987-07-14	ST_CLERK	2400.00	0.00	120	50
128	Steven	Markle	SKING	650.999.1434	1987-07-15	ST_CLERK	2200.00	0.00	120	50
160	Louise	Doran	SKING	011.44.1345.629268	1987-08-16	SA_REP	7500.00	0.30	146	80
161	Sarath	Sewall	SKING	011.44.1345.529268	1987-08-17	SA_REP	7000.00	0.25	146	80
162	Clara	Vishney	SKING	011.44.1346.129268	1987-08-18	SA_REP	10500.00	0.25	147	80
163	Danielle	Greene	SKING	011.44.1346.229268	1987-08-19	SA_REP	9500.00	0.15	147	80
164	Mattea	Marvins	SKING	011.44.1346.329268	1987-08-20	SA_REP	7200.00	0.10	147	80
165	David	Lee	SKING	011.44.1346.529268	1987-08-21	SA_REP	6800.00	0.10	147	80
166	Sundar	Ande	SKING	011.44.1346.629268	1987-08-22	SA_REP	6400.00	0.10	147	80
167	Amit	Banda	SKING	011.44.1346.729268	1987-08-23	SA_REP	6200.00	0.10	147	80
168	Lisa	Ozer	SKING	011.44.1343.929268	1987-08-24	SA_REP	11500.00	0.25	148	80
169	Harrison	Bloom	SKING	011.44.1343.829268	1987-08-25	SA_REP	10000.00	0.20	148	80
170	Tayler	Fox	SKING	011.44.1343.729268	1987-08-26	SA_REP	9600.00	0.20	148	80
171	William	Smith	SKING	011.44.1343.629268	1987-08-27	SA_REP	7400.00	0.15	148	80
172	Elizabeth	Bates	SKING	011.44.1343.529268	1987-08-28	SA_REP	7300.00	0.15	148	80
173	Sundita	Kumar	SKING	011.44.1343.329268	1987-08-29	SA_REP	6100.00	0.10	148	80
174	Ellen	Abel	SKING	011.44.1644.429267	1987-08-30	SA_REP	11000.00	0.30	149	80
175	Alyssa	Hutton	SKING	011.44.1644.429266	1987-08-31	SA_REP	8800.00	0.25	149	80
176	Jonathon	Taylor	SKING	011.44.1644.429265	1987-09-01	SA_REP	8600.00	0.20	149	80
177	Jack	Livingston	SKING	011.44.1644.429264	1987-09-02	SA_REP	8400.00	0.20	149	80
178	Kimberely	Grant	SKING	011.44.1644.429263	1987-09-03	SA_REP	7000.00	0.15	149	0
179	Charles	Johnson	SKING	011.44.1644.429262	1987-09-04	SA_REP	6200.00	0.10	149	80
180	Winston	Taylor	SKING	650.507.9876	1987-09-05	SH_CLERK	3200.00	0.00	120	50
181	Jean	Fleaur	SKING	650.507.9877	1987-09-06	SH_CLERK	3100.00	0.00	120	50
182	Martha	Sullivan	SKING	650.507.9878	1987-09-07	SH_CLERK	2500.00	0.00	120	50
183	Girard	Geoni	SKING	650.507.9879	1987-09-08	SH_CLERK	2800.00	0.00	120	50
184	Nandita	Sarchand	SKING	650.509.1876	1987-09-09	SH_CLERK	4200.00	0.00	121	50
185	Alexis	Bull	SKING	650.509.2876	1987-09-10	SH_CLERK	4100.00	0.00	121	50
186	Julia	Dellinger	SKING	650.509.3876	1987-09-11	SH_CLERK	3400.00	0.00	121	50
187	Anthony	Cabrio	SKING	650.509.4876	1987-09-12	SH_CLERK	3000.00	0.00	121	50
188	Kelly	Chung	SKING	650.505.1876	1987-09-13	SH_CLERK	3800.00	0.00	122	50
189	Jennifer	Dilly	SKING	650.505.2876	1987-09-14	SH_CLERK	3600.00	0.00	122	50
190	Timothy	Gates	SKING	650.505.3876	1987-09-15	SH_CLERK	2900.00	0.00	122	50
191	Randall	Perkins	SKING	650.505.4876	1987-09-16	SH_CLERK	2500.00	0.00	122	50
192	Sarah	Bell	SKING	650.501.1876	1987-09-17	SH_CLERK	4000.00	0.00	123	50
193	Britney	Everett	SKING	650.501.2876	1987-09-18	SH_CLERK	3900.00	0.00	123	50
194	Samuel	McCain	SKING	650.501.3876	1987-09-19	SH_CLERK	3200.00	0.00	123	50
195	Vance	Jones	SKING	650.501.4876	1987-09-20	SH_CLERK	2800.00	0.00	123	50
196	Alana	Walsh	SKING	650.507.9811	1987-09-21	SH_CLERK	3100.00	0.00	124	50
197	Kevin	Feeney	SKING	650.507.9822	1987-09-22	SH_CLERK	3000.00	0.00	124	50
198	Donald	OConnell	SKING	650.507.9833	1987-09-23	SH_CLERK	2600.00	0.00	124	50
199	Douglas	Grant	SKING	650.507.9844	1987-09-24	SH_CLERK	2600.00	0.00	124	50
200	Jennifer	Whalen	SKING	515.123.4444	1987-09-25	AD_ASST	4400.00	0.00	101	10
201	Michael	Hartstein	SKING	515.123.5555	1987-09-26	MK_MAN	13000.00	0.00	100	20
202	Pat	Fay	SKING	603.123.6666	1987-09-27	MK_REP	6000.00	0.00	201	20
203	Susan	Mavris	SKING	515.123.7777	1987-09-28	HR_REP	6500.00	0.00	101	40
204	Hermann	Baer	SKING	515.123.8888	1987-09-29	PR_REP	10000.00	0.00	101	70
205	Shelley	Higgins	SKING	515.123.8080	1987-09-30	AC_MGR	12000.00	0.00	101	110
206	William	Gietz	SKING	515.123.8181	1987-10-01	AC_ACCOUNT	8300.00	0.00	205	110
\.


--
-- Data for Name: job_grades; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY job_grades (grade_level, lowest_sal, highest_sal) FROM stdin;
A 	1000	2999
B 	3000	5999
C 	6000	9999
D 	10000	14999
E 	15000	24999
F 	25000	40000
\.


--
-- Data for Name: job_history; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY job_history (employee_id, start_date, end_date, job_id, department_id) FROM stdin;
102	1993-01-13	1998-07-24	IT_PROG	60
101	1989-09-21	1993-10-27	AC_ACCOUNT	110
101	1993-10-28	1997-03-15	AC_MGR	110
201	1996-02-17	1999-12-19	MK_REP	20
114	1998-03-24	1999-12-31	ST_CLERK	50
122	1999-01-01	1999-12-31	ST_CLERK	50
200	1987-09-17	1993-06-17	AD_ASST	90
176	1998-03-24	1998-12-31	SA_REP	80
176	1999-01-01	1999-12-31	SA_MAN	80
200	1994-07-01	1998-12-31	AC_ACCOUNT	90
\.


--
-- Data for Name: jobs; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY jobs (job_id, job_title, min_salary, max_salary) FROM stdin;
AD_PRES	President	20000	40000
AD_VP	Administration Vice President	15000	30000
AD_ASST	Administration Assistant	3000	6000
FI_MGR	Finance Manager	8200	16000
FI_ACCOUNT	Accountant	4200	9000
AC_MGR	Accounting Manager	8200	16000
AC_ACCOUNT	Public Accountant	4200	9000
SA_MAN	Sales Manager	10000	20000
SA_REP	Sales Representative	6000	12000
PU_MAN	Purchasing Manager	8000	15000
PU_CLERK	Purchasing Clerk	2500	5500
ST_MAN	Stock Manager	5500	8500
ST_CLERK	Stock Clerk	2000	5000
SH_CLERK	Shipping Clerk	2500	5500
IT_PROG	Programmer	4000	10000
MK_MAN	Marketing Manager	9000	15000
MK_REP	Marketing Representative	4000	9000
HR_REP	Human Resources Representative	4000	9000
PR_REP	Public Relations Representative	4500	10500
\.


--
-- Data for Name: locations; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY locations (location_id, street_address, postal_code, city, state_province, country_id) FROM stdin;
1000	1297 Via Cola di Rie	989	Roma		IT
1100	93091 Calle della Testa	10934	Venice		IT
1200	2017 Shinjuku-ku	1689	Tokyo	Tokyo Prefecture	JP
1300	9450 Kamiya-cho	6823	Hiroshima		JP
1400	2014 Jabberwocky Rd	26192	Southlake	Texas	US
1500	2011 Interiors Blvd	99236	South San Francisco	California	US
1600	2007 Zagora St	50090	South Brunswick	New Jersey	US
1700	2004 Charade Rd	98199	Seattle	Washington	US
1800	147 Spadina Ave	M5V 2L7	Toronto	Ontario	CA
1900	6092 Boxwood St	YSW 9T2	Whitehorse	Yukon	CA
2000	40-5-12 Laogianggen	190518	Beijing		CN
2100	1298 Vileparle (E)	490231	Bombay	Maharashtra	IN
2200	12-98 Victoria Street	2901	Sydney	New South Wales	AU
2300	198 Clementi North	540198	Singapore		SG
2400	8204 Arthur St		London		UK
2500	"Magdalen Centre	 The Oxford 	OX9 9ZB	Oxford	Ox
2600	9702 Chester Road	9629850293	Stretford	Manchester	UK
2700	Schwanthalerstr. 7031	80925	Munich	Bavaria	DE
2800	Rua Frei Caneca 1360	01307-002	Sao Paulo	Sao Paulo	BR
2900	20 Rue des Corps-Saints	1730	Geneva	Geneve	CH
3000	Murtenstrasse 921	3095	Bern	BE	CH
3100	Pieter Breughelstraat 837	3029SK	Utrecht	Utrecht	NL
3200	Mariano Escobedo 9991	11932	Mexico City	"Distrito Federal	"
\.


--
-- Data for Name: myresidents; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY myresidents (first_name, last_name, unit, status, market_rent) FROM stdin;
frank     	william   	10        	Current        	1500.00
frank     	william   	10        	Current        	1500.00
james     	leed      	20        	current        	1600.00
\.


--
-- Data for Name: new_table; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY new_table (country_id, country_name, region_id) FROM stdin;
C1	India	1002
C2	USA	\N
\N	UK	\N
C4	India	1001
C5	USA	1007
C6	UK	1003
\.


--
-- Data for Name: number_employees; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY number_employees (department_id, no_employees) FROM stdin;
70	1
20	2
40	1
30	6
60	4
10	1
80	34
50	45
90	2
110	2
100	6
\.


--
-- Data for Name: numberofemployees; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY numberofemployees (department_id, no_employees) FROM stdin;
70	1
20	2
40	1
30	6
60	4
10	1
80	34
50	45
90	2
110	2
100	6
\.


--
-- Data for Name: persons; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY persons (p_id, lastname, firstname, address, city) FROM stdin;
105	sh	mn	154 street	sandnes
\.


--
-- Data for Name: personsnotnull; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY personsnotnull (p_id, lastname, firstname, address, city) FROM stdin;
100	james	syed	252 street	pune
12	nick	ritch	150 street	New York
\.


--
-- Data for Name: regions; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY regions (region_id, region_name) FROM stdin;
1	Europe                   
2	Americas                 
3	Asia                     
4	Middle East and Africa   
\.


--
-- Data for Name: residents; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY residents (first_name, last_name, unit, status, market_rent) FROM stdin;
frank     	william   	10        	Current        	1500.00
frank     	william   	10        	Current        	1500.00
james     	leed      	20        	current        	1600.00
frank     	william   	10        	Current        	1500.00
frank     	william   	10        	Current        	1500.00
james     	leed      	20        	current        	1600.00
\.


--
-- Data for Name: school; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY school (emp_name, age, salary, class) FROM stdin;
sarita	25	24000	1
vineeta	35	29000	2
varun	30	40000	3
\.


--
-- Data for Name: temp_employee; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY temp_employee (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id) FROM stdin;
101	Neena	Kochhar	not 	515.123.4568	1987-06-18	AD_VP	17000.00	0.31	100	90
102	Lex	De Haan	not 	515.123.4569	1987-06-19	AD_VP	17000.00	0.31	100	90
177	Jack	Livingston	not 	011.44.1644.429264	1987-09-02	SA_REP	8400.00	0.31	149	80
178	Kimberely	Grant	not 	011.44.1644.429263	1987-09-03	SA_REP	7000.00	0.31	149	0
179	Charles	Johnson	not 	011.44.1644.429262	1987-09-04	SA_REP	6200.00	0.31	149	80
180	Winston	Taylor	not 	650.507.9876	1987-09-05	SH_CLERK	3200.00	0.31	120	50
181	Jean	Fleaur	not 	650.507.9877	1987-09-06	SH_CLERK	3100.00	0.31	120	50
182	Martha	Sullivan	not 	650.507.9878	1987-09-07	SH_CLERK	2500.00	0.31	120	50
103	Alexander	Hunold	not 	590.423.4567	1987-06-20	IT_PROG	9000.00	0.31	102	60
104	Bruce	Ernst	not 	590.423.4568	1987-06-21	IT_PROG	6000.00	0.31	103	60
106	Valli	Pataballa	not 	590.423.4560	1987-06-23	IT_PROG	4800.00	0.31	103	60
107	Diana	Lorentz	not 	590.423.5567	1987-06-24	IT_PROG	4200.00	0.31	103	60
108	Nancy	Greenberg	not 	515.124.4569	1987-06-25	FI_MGR	12000.00	0.31	101	100
109	Daniel	Faviet	not 	515.124.4169	1987-06-26	FI_ACCOUNT	9000.00	0.31	108	100
110	John	Chen	not 	515.124.4269	1987-06-27	FI_ACCOUNT	8200.00	0.31	108	100
111	Ismael	Sciarra	not 	515.124.4369	1987-06-28	FI_ACCOUNT	7700.00	0.31	108	100
112	Jose Manuel	Urman	not 	515.124.4469	1987-06-29	FI_ACCOUNT	7800.00	0.31	108	100
113	Luis	Popp	not 	515.124.4567	1987-06-30	FI_ACCOUNT	6900.00	0.31	108	100
114	Den	Raphaely	not 	515.127.4561	1987-07-01	PU_MAN	11000.00	0.31	100	30
115	Alexander	Khoo	not 	515.127.4562	1987-07-02	PU_CLERK	3100.00	0.31	114	30
116	Shelli	Baida	not 	515.127.4563	1987-07-03	PU_CLERK	2900.00	0.31	114	30
117	Sigal	Tobias	not 	515.127.4564	1987-07-04	PU_CLERK	2800.00	0.31	114	30
119	Karen	Colmenares	not 	515.127.4566	1987-07-06	PU_CLERK	2500.00	0.31	114	30
120	Matthew	Weiss	not 	650.123.1234	1987-07-07	ST_MAN	8000.00	0.31	100	50
121	Adam	Fripp	not 	650.123.2234	1987-07-08	ST_MAN	8200.00	0.31	100	50
122	Payam	Kaufling	not 	650.123.3234	1987-07-09	ST_MAN	7900.00	0.31	100	50
123	Shanta	Vollman	not 	650.123.4234	1987-07-10	ST_MAN	6500.00	0.31	100	50
118	Guy	Himuro	not 	515.127.4565	1987-07-05	SH_CLERK	2600.00	0.31	114	30
124	Kevin	Mourgos	not 	650.123.5234	1987-07-11	ST_MAN	5800.00	0.31	100	50
125	Julia	Nayer	not 	650.124.1214	1987-07-12	ST_CLERK	3200.00	0.31	120	50
126	Irene	Mikkilineni	not 	650.124.1224	1987-07-13	ST_CLERK	2700.00	0.31	120	50
127	James	Landry	not 	650.124.1334	1987-07-14	ST_CLERK	2400.00	0.31	120	50
128	Steven	Markle	not 	650.124.1434	1987-07-15	ST_CLERK	2200.00	0.31	120	50
129	Laura	Bissot	not 	650.124.5234	1987-07-16	ST_CLERK	3300.00	0.31	121	50
130	Mozhe	Atkinson	not 	650.124.6234	1987-07-17	ST_CLERK	2800.00	0.31	121	50
131	James	Marlow	not 	650.124.7234	1987-07-18	ST_CLERK	2500.00	0.31	121	50
132	TJ	Olson	not 	650.124.8234	1987-07-19	ST_CLERK	2100.00	0.31	121	50
133	Jason	Mallin	not 	650.127.1934	1987-07-20	ST_CLERK	3300.00	0.31	122	50
134	Michael	Rogers	not 	650.127.1834	1987-07-21	ST_CLERK	2900.00	0.31	122	50
135	Ki	Gee	not 	650.127.1734	1987-07-22	ST_CLERK	2400.00	0.31	122	50
136	Hazel	Philtanker	not 	650.127.1634	1987-07-23	ST_CLERK	2200.00	0.31	122	50
137	Renske	Ladwig	not 	650.121.1234	1987-07-24	ST_CLERK	3600.00	0.31	123	50
138	Stephen	Stiles	not 	650.121.2034	1987-07-25	ST_CLERK	3200.00	0.31	123	50
139	John	Seo	not 	650.121.2019	1987-07-26	ST_CLERK	2700.00	0.31	123	50
140	Joshua	Patel	not 	650.121.1834	1987-07-27	ST_CLERK	2500.00	0.31	123	50
141	Trenna	Rajs	not 	650.121.8009	1987-07-28	ST_CLERK	3500.00	0.31	124	50
142	Curtis	Davies	not 	650.121.2994	1987-07-29	ST_CLERK	3100.00	0.31	124	50
202	Pat	Fay	not 	603.123.6666	1987-09-27	MK_REP	6000.00	0.31	201	20
143	Randall	Matos	not 	650.121.2874	1987-07-30	ST_CLERK	2600.00	0.31	124	50
144	Peter	Vargas	not 	650.121.2004	1987-07-31	ST_CLERK	2500.00	0.31	124	50
145	John	Russell	not 	011.44.1344.429268	1987-08-01	SA_MAN	14000.00	0.31	100	80
146	Karen	Partners	not 	011.44.1344.467268	1987-08-02	SA_MAN	13500.00	0.31	100	80
147	Alberto	Errazuriz	not 	011.44.1344.429278	1987-08-03	SA_MAN	12000.00	0.31	100	80
148	Gerald	Cambrault	not 	011.44.1344.619268	1987-08-04	SA_MAN	11000.00	0.31	100	80
149	Eleni	Zlotkey	not 	011.44.1344.429018	1987-08-05	SA_MAN	10500.00	0.31	100	80
150	Peter	Tucker	not 	011.44.1344.129268	1987-08-06	SA_REP	10000.00	0.31	145	80
151	David	Bernstein	not 	011.44.1344.345268	1987-08-07	SA_REP	9500.00	0.31	145	80
152	Peter	Hall	not 	011.44.1344.478968	1987-08-08	SA_REP	9000.00	0.31	145	80
153	Christopher	Olsen	not 	011.44.1344.498718	1987-08-09	SA_REP	8000.00	0.31	145	80
154	Nanette	Cambrault	not 	011.44.1344.987668	1987-08-10	SA_REP	7500.00	0.31	145	80
155	Oliver	Tuvault	not 	011.44.1344.486508	1987-08-11	SA_REP	7000.00	0.31	145	80
156	Janette	King	not 	011.44.1345.429268	1987-08-12	SA_REP	10000.00	0.31	146	80
157	Patrick	Sully	not 	011.44.1345.929268	1987-08-13	SA_REP	9500.00	0.31	146	80
158	Allan	McEwen	not 	011.44.1345.829268	1987-08-14	SA_REP	9000.00	0.31	146	80
159	Lindsey	Smith	not 	011.44.1345.729268	1987-08-15	SA_REP	8000.00	0.31	146	80
160	Louise	Doran	not 	011.44.1345.629268	1987-08-16	SA_REP	7500.00	0.31	146	80
161	Sarath	Sewall	not 	011.44.1345.529268	1987-08-17	SA_REP	7000.00	0.31	146	80
162	Clara	Vishney	not 	011.44.1346.129268	1987-08-18	SA_REP	10500.00	0.31	147	80
163	Danielle	Greene	not 	011.44.1346.229268	1987-08-19	SA_REP	9500.00	0.31	147	80
164	Mattea	Marvins	not 	011.44.1346.329268	1987-08-20	SA_REP	7200.00	0.31	147	80
165	David	Lee	not 	011.44.1346.529268	1987-08-21	SA_REP	6800.00	0.31	147	80
166	Sundar	Ande	not 	011.44.1346.629268	1987-08-22	SA_REP	6400.00	0.31	147	80
167	Amit	Banda	not 	011.44.1346.729268	1987-08-23	SA_REP	6200.00	0.31	147	80
168	Lisa	Ozer	not 	011.44.1343.929268	1987-08-24	SA_REP	11500.00	0.31	148	80
169	Harrison	Bloom	not 	011.44.1343.829268	1987-08-25	SA_REP	10000.00	0.31	148	80
170	Tayler	Fox	not 	011.44.1343.729268	1987-08-26	SA_REP	9600.00	0.31	148	80
171	William	Smith	not 	011.44.1343.629268	1987-08-27	SA_REP	7400.00	0.31	148	80
172	Elizabeth	Bates	not 	011.44.1343.529268	1987-08-28	SA_REP	7300.00	0.31	148	80
173	Sundita	Kumar	not 	011.44.1343.329268	1987-08-29	SA_REP	6100.00	0.31	148	80
174	Ellen	Abel	not 	011.44.1644.429267	1987-08-30	SA_REP	11000.00	0.31	149	80
175	Alyssa	Hutton	not 	011.44.1644.429266	1987-08-31	SA_REP	8800.00	0.31	149	80
176	Jonathon	Taylor	not 	011.44.1644.429265	1987-09-01	SA_REP	8600.00	0.31	149	80
183	Girard	Geoni	not 	650.507.9879	1987-09-08	SH_CLERK	2800.00	0.31	120	50
184	Nandita	Sarchand	not 	650.509.1876	1987-09-09	SH_CLERK	4200.00	0.31	121	50
185	Alexis	Bull	not 	650.509.2876	1987-09-10	SH_CLERK	4100.00	0.31	121	50
186	Julia	Dellinger	not 	650.509.3876	1987-09-11	SH_CLERK	3400.00	0.31	121	50
187	Anthony	Cabrio	not 	650.509.4876	1987-09-12	SH_CLERK	3000.00	0.31	121	50
188	Kelly	Chung	not 	650.505.1876	1987-09-13	SH_CLERK	3800.00	0.31	122	50
189	Jennifer	Dilly	not 	650.505.2876	1987-09-14	SH_CLERK	3600.00	0.31	122	50
190	Timothy	Gates	not 	650.505.3876	1987-09-15	SH_CLERK	2900.00	0.31	122	50
191	Randall	Perkins	not 	650.505.4876	1987-09-16	SH_CLERK	2500.00	0.31	122	50
192	Sarah	Bell	not 	650.501.1876	1987-09-17	SH_CLERK	4000.00	0.31	123	50
193	Britney	Everett	not 	650.501.2876	1987-09-18	SH_CLERK	3900.00	0.31	123	50
194	Samuel	McCain	not 	650.501.3876	1987-09-19	SH_CLERK	3200.00	0.31	123	50
195	Vance	Jones	not 	650.501.4876	1987-09-20	SH_CLERK	2800.00	0.31	123	50
196	Alana	Walsh	not 	650.507.9811	1987-09-21	SH_CLERK	3100.00	0.31	124	50
197	Kevin	Feeney	not 	650.507.9822	1987-09-22	SH_CLERK	3000.00	0.31	124	50
198	Donald	OConnell	not 	650.507.9833	1987-09-23	SH_CLERK	2600.00	0.31	124	50
199	Douglas	Grant	not 	650.507.9844	1987-09-24	SH_CLERK	2600.00	0.31	124	50
200	Jennifer	Whalen	not 	515.123.4444	1987-09-25	AD_ASST	4400.00	0.31	101	10
201	Michael	Hartstein	not 	515.123.5555	1987-09-26	MK_MAN	13000.00	0.31	100	20
203	Susan	Mavris	not 	515.123.7777	1987-09-28	HR_REP	6500.00	0.31	101	40
204	Hermann	Baer	not 	515.123.8888	1987-09-29	PR_REP	10000.00	0.31	101	70
205	Shelley	Higgins	not available	515.123.8080	1987-09-30	AC_MGR	12000.00	0.10	101	110
206	William	Gietz	not available	515.123.8181	1987-10-01	AC_ACCOUNT	8300.00	0.10	205	110
\.


--
-- Data for Name: temptable; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY temptable (employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id) FROM stdin;
101	Neena	Kochhar	not 	515.123.4568	1987-06-18	AD_VP	17000.00	0.31	100	90
102	Lex	De Haan	not 	515.123.4569	1987-06-19	AD_VP	17000.00	0.31	100	90
177	Jack	Livingston	not 	011.44.1644.429264	1987-09-02	SA_REP	8400.00	0.31	149	80
178	Kimberely	Grant	not 	011.44.1644.429263	1987-09-03	SA_REP	7000.00	0.31	149	0
179	Charles	Johnson	not 	011.44.1644.429262	1987-09-04	SA_REP	6200.00	0.31	149	80
180	Winston	Taylor	not 	650.507.9876	1987-09-05	SH_CLERK	3200.00	0.31	120	50
181	Jean	Fleaur	not 	650.507.9877	1987-09-06	SH_CLERK	3100.00	0.31	120	50
182	Martha	Sullivan	not 	650.507.9878	1987-09-07	SH_CLERK	2500.00	0.31	120	50
103	Alexander	Hunold	not 	590.423.4567	1987-06-20	IT_PROG	9000.00	0.31	102	60
104	Bruce	Ernst	not 	590.423.4568	1987-06-21	IT_PROG	6000.00	0.31	103	60
106	Valli	Pataballa	not 	590.423.4560	1987-06-23	IT_PROG	4800.00	0.31	103	60
107	Diana	Lorentz	not 	590.423.5567	1987-06-24	IT_PROG	4200.00	0.31	103	60
108	Nancy	Greenberg	not 	515.124.4569	1987-06-25	FI_MGR	12000.00	0.31	101	100
109	Daniel	Faviet	not 	515.124.4169	1987-06-26	FI_ACCOUNT	9000.00	0.31	108	100
110	John	Chen	not 	515.124.4269	1987-06-27	FI_ACCOUNT	8200.00	0.31	108	100
111	Ismael	Sciarra	not 	515.124.4369	1987-06-28	FI_ACCOUNT	7700.00	0.31	108	100
112	Jose Manuel	Urman	not 	515.124.4469	1987-06-29	FI_ACCOUNT	7800.00	0.31	108	100
113	Luis	Popp	not 	515.124.4567	1987-06-30	FI_ACCOUNT	6900.00	0.31	108	100
114	Den	Raphaely	not 	515.127.4561	1987-07-01	PU_MAN	11000.00	0.31	100	30
115	Alexander	Khoo	not 	515.127.4562	1987-07-02	PU_CLERK	3100.00	0.31	114	30
116	Shelli	Baida	not 	515.127.4563	1987-07-03	PU_CLERK	2900.00	0.31	114	30
117	Sigal	Tobias	not 	515.127.4564	1987-07-04	PU_CLERK	2800.00	0.31	114	30
119	Karen	Colmenares	not 	515.127.4566	1987-07-06	PU_CLERK	2500.00	0.31	114	30
120	Matthew	Weiss	not 	650.123.1234	1987-07-07	ST_MAN	8000.00	0.31	100	50
121	Adam	Fripp	not 	650.123.2234	1987-07-08	ST_MAN	8200.00	0.31	100	50
122	Payam	Kaufling	not 	650.123.3234	1987-07-09	ST_MAN	7900.00	0.31	100	50
123	Shanta	Vollman	not 	650.123.4234	1987-07-10	ST_MAN	6500.00	0.31	100	50
118	Guy	Himuro	not 	515.127.4565	1987-07-05	SH_CLERK	2600.00	0.31	114	30
124	Kevin	Mourgos	not 	650.123.5234	1987-07-11	ST_MAN	5800.00	0.31	100	50
125	Julia	Nayer	not 	650.124.1214	1987-07-12	ST_CLERK	3200.00	0.31	120	50
126	Irene	Mikkilineni	not 	650.124.1224	1987-07-13	ST_CLERK	2700.00	0.31	120	50
127	James	Landry	not 	650.124.1334	1987-07-14	ST_CLERK	2400.00	0.31	120	50
128	Steven	Markle	not 	650.124.1434	1987-07-15	ST_CLERK	2200.00	0.31	120	50
129	Laura	Bissot	not 	650.124.5234	1987-07-16	ST_CLERK	3300.00	0.31	121	50
130	Mozhe	Atkinson	not 	650.124.6234	1987-07-17	ST_CLERK	2800.00	0.31	121	50
131	James	Marlow	not 	650.124.7234	1987-07-18	ST_CLERK	2500.00	0.31	121	50
132	TJ	Olson	not 	650.124.8234	1987-07-19	ST_CLERK	2100.00	0.31	121	50
133	Jason	Mallin	not 	650.127.1934	1987-07-20	ST_CLERK	3300.00	0.31	122	50
134	Michael	Rogers	not 	650.127.1834	1987-07-21	ST_CLERK	2900.00	0.31	122	50
135	Ki	Gee	not 	650.127.1734	1987-07-22	ST_CLERK	2400.00	0.31	122	50
136	Hazel	Philtanker	not 	650.127.1634	1987-07-23	ST_CLERK	2200.00	0.31	122	50
137	Renske	Ladwig	not 	650.121.1234	1987-07-24	ST_CLERK	3600.00	0.31	123	50
138	Stephen	Stiles	not 	650.121.2034	1987-07-25	ST_CLERK	3200.00	0.31	123	50
139	John	Seo	not 	650.121.2019	1987-07-26	ST_CLERK	2700.00	0.31	123	50
140	Joshua	Patel	not 	650.121.1834	1987-07-27	ST_CLERK	2500.00	0.31	123	50
141	Trenna	Rajs	not 	650.121.8009	1987-07-28	ST_CLERK	3500.00	0.31	124	50
142	Curtis	Davies	not 	650.121.2994	1987-07-29	ST_CLERK	3100.00	0.31	124	50
202	Pat	Fay	not 	603.123.6666	1987-09-27	MK_REP	6000.00	0.31	201	20
143	Randall	Matos	not 	650.121.2874	1987-07-30	ST_CLERK	2600.00	0.31	124	50
144	Peter	Vargas	not 	650.121.2004	1987-07-31	ST_CLERK	2500.00	0.31	124	50
145	John	Russell	not 	011.44.1344.429268	1987-08-01	SA_MAN	14000.00	0.31	100	80
146	Karen	Partners	not 	011.44.1344.467268	1987-08-02	SA_MAN	13500.00	0.31	100	80
147	Alberto	Errazuriz	not 	011.44.1344.429278	1987-08-03	SA_MAN	12000.00	0.31	100	80
148	Gerald	Cambrault	not 	011.44.1344.619268	1987-08-04	SA_MAN	11000.00	0.31	100	80
149	Eleni	Zlotkey	not 	011.44.1344.429018	1987-08-05	SA_MAN	10500.00	0.31	100	80
150	Peter	Tucker	not 	011.44.1344.129268	1987-08-06	SA_REP	10000.00	0.31	145	80
151	David	Bernstein	not 	011.44.1344.345268	1987-08-07	SA_REP	9500.00	0.31	145	80
152	Peter	Hall	not 	011.44.1344.478968	1987-08-08	SA_REP	9000.00	0.31	145	80
153	Christopher	Olsen	not 	011.44.1344.498718	1987-08-09	SA_REP	8000.00	0.31	145	80
154	Nanette	Cambrault	not 	011.44.1344.987668	1987-08-10	SA_REP	7500.00	0.31	145	80
155	Oliver	Tuvault	not 	011.44.1344.486508	1987-08-11	SA_REP	7000.00	0.31	145	80
156	Janette	King	not 	011.44.1345.429268	1987-08-12	SA_REP	10000.00	0.31	146	80
157	Patrick	Sully	not 	011.44.1345.929268	1987-08-13	SA_REP	9500.00	0.31	146	80
158	Allan	McEwen	not 	011.44.1345.829268	1987-08-14	SA_REP	9000.00	0.31	146	80
159	Lindsey	Smith	not 	011.44.1345.729268	1987-08-15	SA_REP	8000.00	0.31	146	80
160	Louise	Doran	not 	011.44.1345.629268	1987-08-16	SA_REP	7500.00	0.31	146	80
161	Sarath	Sewall	not 	011.44.1345.529268	1987-08-17	SA_REP	7000.00	0.31	146	80
162	Clara	Vishney	not 	011.44.1346.129268	1987-08-18	SA_REP	10500.00	0.31	147	80
163	Danielle	Greene	not 	011.44.1346.229268	1987-08-19	SA_REP	9500.00	0.31	147	80
164	Mattea	Marvins	not 	011.44.1346.329268	1987-08-20	SA_REP	7200.00	0.31	147	80
165	David	Lee	not 	011.44.1346.529268	1987-08-21	SA_REP	6800.00	0.31	147	80
166	Sundar	Ande	not 	011.44.1346.629268	1987-08-22	SA_REP	6400.00	0.31	147	80
167	Amit	Banda	not 	011.44.1346.729268	1987-08-23	SA_REP	6200.00	0.31	147	80
168	Lisa	Ozer	not 	011.44.1343.929268	1987-08-24	SA_REP	11500.00	0.31	148	80
169	Harrison	Bloom	not 	011.44.1343.829268	1987-08-25	SA_REP	10000.00	0.31	148	80
170	Tayler	Fox	not 	011.44.1343.729268	1987-08-26	SA_REP	9600.00	0.31	148	80
171	William	Smith	not 	011.44.1343.629268	1987-08-27	SA_REP	7400.00	0.31	148	80
172	Elizabeth	Bates	not 	011.44.1343.529268	1987-08-28	SA_REP	7300.00	0.31	148	80
173	Sundita	Kumar	not 	011.44.1343.329268	1987-08-29	SA_REP	6100.00	0.31	148	80
174	Ellen	Abel	not 	011.44.1644.429267	1987-08-30	SA_REP	11000.00	0.31	149	80
175	Alyssa	Hutton	not 	011.44.1644.429266	1987-08-31	SA_REP	8800.00	0.31	149	80
176	Jonathon	Taylor	not 	011.44.1644.429265	1987-09-01	SA_REP	8600.00	0.31	149	80
183	Girard	Geoni	not 	650.507.9879	1987-09-08	SH_CLERK	2800.00	0.31	120	50
184	Nandita	Sarchand	not 	650.509.1876	1987-09-09	SH_CLERK	4200.00	0.31	121	50
185	Alexis	Bull	not 	650.509.2876	1987-09-10	SH_CLERK	4100.00	0.31	121	50
186	Julia	Dellinger	not 	650.509.3876	1987-09-11	SH_CLERK	3400.00	0.31	121	50
187	Anthony	Cabrio	not 	650.509.4876	1987-09-12	SH_CLERK	3000.00	0.31	121	50
188	Kelly	Chung	not 	650.505.1876	1987-09-13	SH_CLERK	3800.00	0.31	122	50
189	Jennifer	Dilly	not 	650.505.2876	1987-09-14	SH_CLERK	3600.00	0.31	122	50
190	Timothy	Gates	not 	650.505.3876	1987-09-15	SH_CLERK	2900.00	0.31	122	50
191	Randall	Perkins	not 	650.505.4876	1987-09-16	SH_CLERK	2500.00	0.31	122	50
192	Sarah	Bell	not 	650.501.1876	1987-09-17	SH_CLERK	4000.00	0.31	123	50
193	Britney	Everett	not 	650.501.2876	1987-09-18	SH_CLERK	3900.00	0.31	123	50
194	Samuel	McCain	not 	650.501.3876	1987-09-19	SH_CLERK	3200.00	0.31	123	50
195	Vance	Jones	not 	650.501.4876	1987-09-20	SH_CLERK	2800.00	0.31	123	50
196	Alana	Walsh	not 	650.507.9811	1987-09-21	SH_CLERK	3100.00	0.31	124	50
197	Kevin	Feeney	not 	650.507.9822	1987-09-22	SH_CLERK	3000.00	0.31	124	50
198	Donald	OConnell	not 	650.507.9833	1987-09-23	SH_CLERK	2600.00	0.31	124	50
199	Douglas	Grant	not 	650.507.9844	1987-09-24	SH_CLERK	2600.00	0.31	124	50
200	Jennifer	Whalen	not 	515.123.4444	1987-09-25	AD_ASST	4400.00	0.31	101	10
201	Michael	Hartstein	not 	515.123.5555	1987-09-26	MK_MAN	13000.00	0.31	100	20
203	Susan	Mavris	not 	515.123.7777	1987-09-28	HR_REP	6500.00	0.31	101	40
204	Hermann	Baer	not 	515.123.8888	1987-09-29	PR_REP	10000.00	0.31	101	70
205	Shelley	Higgins	not available	515.123.8080	1987-09-30	AC_MGR	12000.00	0.10	101	110
206	William	Gietz	not available	515.123.8181	1987-10-01	AC_ACCOUNT	8300.00	0.10	205	110
\.


--
-- Data for Name: test; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY test (name) FROM stdin;
\.


--
-- Name: countries_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY countries
    ADD CONSTRAINT countries_pkey PRIMARY KEY (country_id);


--
-- Name: departments_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY departments
    ADD CONSTRAINT departments_pkey PRIMARY KEY (department_id);


--
-- Name: employees_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY employees
    ADD CONSTRAINT employees_pkey PRIMARY KEY (employee_id);


--
-- Name: job_grades_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY job_grades
    ADD CONSTRAINT job_grades_pkey PRIMARY KEY (grade_level);


--
-- Name: job_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY job_history
    ADD CONSTRAINT job_history_pkey PRIMARY KEY (employee_id, start_date);


--
-- Name: jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (job_id);


--
-- Name: locations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY locations
    ADD CONSTRAINT locations_pkey PRIMARY KEY (location_id);


--
-- Name: regions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY regions
    ADD CONSTRAINT regions_pkey PRIMARY KEY (region_id);


--
-- Name: p1index; Type: INDEX; Schema: public; Owner: postgres; Tablespace: 
--

CREATE INDEX p1index ON persons USING btree (lastname, firstname);


--
-- Name: pindex; Type: INDEX; Schema: public; Owner: postgres; Tablespace: 
--

CREATE INDEX pindex ON persons USING btree (lastname, firstname);


--
-- Name: countries_region_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY countries
    ADD CONSTRAINT countries_region_id_fkey FOREIGN KEY (region_id) REFERENCES regions(region_id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;
GRANT USAGE ON SCHEMA public TO user3;


--
-- Name: a; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE a FROM PUBLIC;
REVOKE ALL ON TABLE a FROM postgres;
GRANT ALL ON TABLE a TO postgres;
GRANT SELECT ON TABLE a TO user3;


--
-- Name: countries; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE countries FROM PUBLIC;
REVOKE ALL ON TABLE countries FROM postgres;
GRANT ALL ON TABLE countries TO postgres;
GRANT SELECT ON TABLE countries TO user3;


--
-- Name: countries1; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE countries1 FROM PUBLIC;
REVOKE ALL ON TABLE countries1 FROM postgres;
GRANT ALL ON TABLE countries1 TO postgres;
GRANT SELECT ON TABLE countries1 TO user3;


--
-- Name: countries123; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE countries123 FROM PUBLIC;
REVOKE ALL ON TABLE countries123 FROM postgres;
GRANT ALL ON TABLE countries123 TO postgres;
GRANT SELECT ON TABLE countries123 TO user3;


--
-- Name: countries2; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE countries2 FROM PUBLIC;
REVOKE ALL ON TABLE countries2 FROM postgres;
GRANT ALL ON TABLE countries2 TO postgres;
GRANT SELECT ON TABLE countries2 TO user3;


--
-- Name: country_new; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE country_new FROM PUBLIC;
REVOKE ALL ON TABLE country_new FROM postgres;
GRANT ALL ON TABLE country_new TO postgres;
GRANT SELECT ON TABLE country_new TO user3;


--
-- Name: country_new123; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE country_new123 FROM PUBLIC;
REVOKE ALL ON TABLE country_new123 FROM postgres;
GRANT ALL ON TABLE country_new123 TO postgres;
GRANT SELECT ON TABLE country_new123 TO user3;


--
-- Name: departments; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE departments FROM PUBLIC;
REVOKE ALL ON TABLE departments FROM postgres;
GRANT ALL ON TABLE departments TO postgres;
GRANT SELECT ON TABLE departments TO user3;


--
-- Name: emp; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE emp FROM PUBLIC;
REVOKE ALL ON TABLE emp FROM postgres;
GRANT ALL ON TABLE emp TO postgres;
GRANT SELECT ON TABLE emp TO user3;


--
-- Name: emp1; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE emp1 FROM PUBLIC;
REVOKE ALL ON TABLE emp1 FROM postgres;
GRANT ALL ON TABLE emp1 TO postgres;
GRANT SELECT ON TABLE emp1 TO user3;


--
-- Name: employees; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE employees FROM PUBLIC;
REVOKE ALL ON TABLE employees FROM postgres;
GRANT ALL ON TABLE employees TO postgres;
GRANT SELECT ON TABLE employees TO user3;


--
-- Name: job_grades; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE job_grades FROM PUBLIC;
REVOKE ALL ON TABLE job_grades FROM postgres;
GRANT ALL ON TABLE job_grades TO postgres;
GRANT SELECT ON TABLE job_grades TO user3;


--
-- Name: job_history; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE job_history FROM PUBLIC;
REVOKE ALL ON TABLE job_history FROM postgres;
GRANT ALL ON TABLE job_history TO postgres;
GRANT SELECT ON TABLE job_history TO user3;


--
-- Name: jobs; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE jobs FROM PUBLIC;
REVOKE ALL ON TABLE jobs FROM postgres;
GRANT ALL ON TABLE jobs TO postgres;
GRANT SELECT ON TABLE jobs TO user3;


--
-- Name: locations; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE locations FROM PUBLIC;
REVOKE ALL ON TABLE locations FROM postgres;
GRANT ALL ON TABLE locations TO postgres;
GRANT SELECT ON TABLE locations TO user3;


--
-- Name: max_sal; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE max_sal FROM PUBLIC;
REVOKE ALL ON TABLE max_sal FROM postgres;
GRANT ALL ON TABLE max_sal TO postgres;
GRANT SELECT ON TABLE max_sal TO user3;


--
-- Name: max_salaries; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE max_salaries FROM PUBLIC;
REVOKE ALL ON TABLE max_salaries FROM postgres;
GRANT ALL ON TABLE max_salaries TO postgres;
GRANT SELECT ON TABLE max_salaries TO user3;


--
-- Name: max_salary; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE max_salary FROM PUBLIC;
REVOKE ALL ON TABLE max_salary FROM postgres;
GRANT ALL ON TABLE max_salary TO postgres;
GRANT SELECT ON TABLE max_salary TO user3;


--
-- Name: myresidents; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE myresidents FROM PUBLIC;
REVOKE ALL ON TABLE myresidents FROM postgres;
GRANT ALL ON TABLE myresidents TO postgres;
GRANT SELECT ON TABLE myresidents TO user3;


--
-- Name: new_table; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE new_table FROM PUBLIC;
REVOKE ALL ON TABLE new_table FROM postgres;
GRANT ALL ON TABLE new_table TO postgres;
GRANT SELECT ON TABLE new_table TO user3;


--
-- Name: number_employees; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE number_employees FROM PUBLIC;
REVOKE ALL ON TABLE number_employees FROM postgres;
GRANT ALL ON TABLE number_employees TO postgres;
GRANT SELECT ON TABLE number_employees TO user3;


--
-- Name: numberofemployees; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE numberofemployees FROM PUBLIC;
REVOKE ALL ON TABLE numberofemployees FROM postgres;
GRANT ALL ON TABLE numberofemployees TO postgres;
GRANT SELECT ON TABLE numberofemployees TO user3;


--
-- Name: persons; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE persons FROM PUBLIC;
REVOKE ALL ON TABLE persons FROM postgres;
GRANT ALL ON TABLE persons TO postgres;
GRANT SELECT ON TABLE persons TO user3;


--
-- Name: personsnotnull; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE personsnotnull FROM PUBLIC;
REVOKE ALL ON TABLE personsnotnull FROM postgres;
GRANT ALL ON TABLE personsnotnull TO postgres;
GRANT SELECT ON TABLE personsnotnull TO user3;


--
-- Name: raster_overviews; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE raster_overviews FROM PUBLIC;
REVOKE ALL ON TABLE raster_overviews FROM postgres;
GRANT ALL ON TABLE raster_overviews TO postgres;
GRANT SELECT ON TABLE raster_overviews TO user3;


--
-- Name: regions; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE regions FROM PUBLIC;
REVOKE ALL ON TABLE regions FROM postgres;
GRANT ALL ON TABLE regions TO postgres;
GRANT SELECT ON TABLE regions TO user3;


--
-- Name: residents; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE residents FROM PUBLIC;
REVOKE ALL ON TABLE residents FROM postgres;
GRANT ALL ON TABLE residents TO postgres;
GRANT SELECT ON TABLE residents TO user3;


--
-- Name: school; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE school FROM PUBLIC;
REVOKE ALL ON TABLE school FROM postgres;
GRANT ALL ON TABLE school TO postgres;
GRANT SELECT ON TABLE school TO user3;


--
-- Name: temp_1; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE temp_1 FROM PUBLIC;
REVOKE ALL ON TABLE temp_1 FROM postgres;
GRANT ALL ON TABLE temp_1 TO postgres;
GRANT SELECT ON TABLE temp_1 TO user3;


--
-- Name: temp_employee; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE temp_employee FROM PUBLIC;
REVOKE ALL ON TABLE temp_employee FROM postgres;
GRANT ALL ON TABLE temp_employee TO postgres;
GRANT SELECT ON TABLE temp_employee TO user3;


--
-- Name: temptable; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE temptable FROM PUBLIC;
REVOKE ALL ON TABLE temptable FROM postgres;
GRANT ALL ON TABLE temptable TO postgres;
GRANT SELECT ON TABLE temptable TO user3;


--
-- Name: test; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE test FROM PUBLIC;
REVOKE ALL ON TABLE test FROM postgres;
GRANT ALL ON TABLE test TO postgres;
GRANT SELECT ON TABLE test TO user3;


--
-- PostgreSQL database dump complete
--

