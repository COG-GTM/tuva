{#
    This macro has been modified to work with the variables set in the dbt_project.yml file.
    See https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas for the original macro.

    Mirrors macros/generate_schema_name.sql in the the_tuva_project package: the
    `tuva_schema_prefix` variable is applied centrally so models only need to
    declare their bare schema name. When `tuva_schema_prefix` is set the resolved
    schema becomes `<prefix>_<schema>`; otherwise it is the bare schema.
#}

{% macro default__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set tuva_prefix = var('tuva_schema_prefix', None) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif tuva_prefix is not none -%}
        {{ (tuva_prefix ~ '_' ~ custom_schema_name | trim) | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
