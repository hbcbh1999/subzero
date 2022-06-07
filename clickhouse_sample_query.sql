select
    groupArray(cast(
        tuple(r.id, r.name, r.client, r.tasks),
        concat(
            'Tuple(', 
            'id ', toTypeName(r.id), ',',
            'name ', toTypeName(r.name), ',',
            'client ', toTypeName(r.client), ',',
            'tasks ', toTypeName(r.tasks),
            ')'
        )
    )) as body
from(
    select 
        p.id as id,
        p.name as name, 
        any(cast(tuple(c.id, c.name), concat('Tuple(', 'id ', toTypeName(c.id), ',', 'name ', toTypeName(c.name), ')'))) as client,
        groupArray(cast(tuple(t.id, t.name), concat('Tuple(', 'id ', toTypeName(t.id), ',', 'name ', toTypeName(t.name), ')'))) as tasks
    from projects as p
    left join (
        select id, name from clients
    ) c on p.client_id = c.id
    left join (
        select id, name, project_id from tasks
    ) t on p.id = t.project_id
    --where p.id = 1
    group by p.id, p.name
) r

format JSON
settings output_format_json_named_tuples_as_objects=1;