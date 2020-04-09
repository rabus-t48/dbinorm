# Dbinorm

Database Interface not orm

# The purpose

Implementaion of simple tool that can do working with
database more simple and untuitive.

Example:

for oracle database:

```python

    from dbinorm.connection import GetConnection
    from dbinorm.request import Request

    constr = 'test/test@127.0.0.1:1521/XE'

    REQS = Request({
       'get_id_from_test_table': "select id from test_table"
    })

    with GetConnection(constr) as con:
        req = REQS.get_by_con("get_id_from_test_table", con)
        result = con.uexec_get_list(req) # dict will be returned
        lr = len(result)
        first_id = None
        second_id = None
        if lr > 0:
           first_id = result[0]["ID"]
        if lr > 1:
           second_id = result[0]["ID"]
```