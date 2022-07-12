-- select
--     groupArray(cast(
--         tuple(r.id, r."name", r.client, r.tasks),
--         concat(
--             'Tuple(', 
--             'id ', toTypeName(r.id), ',',
--             'name ', toTypeName(r."name"), ',',
--             'client ', toTypeName(r.client), ',',
--             'tasks ', toTypeName(r.tasks),
--             ')'
--         )
--     )) as body
-- from(
    select 
        "default"."projects"."id" as "id",
        "default"."projects"."name" as "name", 
        any(cast(tuple("projects_clients"."id", "projects_clients"."name"), concat('Tuple(', 'id ', toTypeName("projects_clients"."id"), ',', 'name ', toTypeName("projects_clients"."name"), ')'))) as client,
        groupArray(cast(tuple("projects_tasks"."id", "projects_tasks"."name"), concat('Tuple(', 'id ', toTypeName("projects_tasks"."id"), ',', 'name ', toTypeName("projects_tasks"."name"), ')'))) as tasks
    from "default"."projects"
    left join (
        select 
            "default"."clients"."id" as "id", 
            "default"."clients"."name" as "name"
            from "default"."clients"
    ) as "projects_clients" on "default"."projects"."client_id" = "projects_clients"."id"
    left join (
        select 
            "default"."tasks"."id" as "id",
            "default"."tasks"."name" as "name",
            "default"."tasks"."project_id" as "project_id"
        from "default"."tasks"
    ) as "projects_tasks" on "default"."projects"."id" = "projects_tasks"."project_id"

    --where p.id = 1
    group by "default"."projects"."id", "default"."projects"."name"
-- ) r

-- format JSON
format JSONEachRow
settings 
    output_format_json_named_tuples_as_objects=1,
    join_use_nulls=1,
    output_format_json_array_of_rows=1;