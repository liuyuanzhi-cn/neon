import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.timeout(10000)
def test_gc_old_layers(neon_env_builder: NeonEnvBuilder):
    """
    Test that GC is able to collect all old layers even if them are forming
    "stairs" and there are not three delta layers since last image layer.

    Information about image layers needed to collect old layers should
    be propagated by GC to compaction task which should take in in account
    when make a decision which new image layers needs to be created.
    """
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            # disable default GC and compaction
            "gc_period": "1000 m",
            "compaction_period": "0 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "1 s",
            # "compaction_threshold": "3",
            # "image_creation_threshold": "2",
        }
    )
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]
    n_steps = 10
    n_update_iters = 100
    step_size = 10000
    with pg.cursor() as cur:
        cur.execute("SET statement_timeout='1000s'")
        cur.execute(
            "CREATE TABLE t(pk bigint primary key, count bigint default 0, payload text default repeat(' ', 100))  with (fillfactor=50)"
        )
        for step in range(n_steps):
            cur.execute(
                f"INSERT INTO t (pk) values (generate_series({step*step_size+1},{(step+1)*step_size}))"
            )
            for i in range(n_update_iters):
                cur.execute(
                    f"UPDATE t set count=count+1 where pk BETWEEN {(step-1)*step_size+1+i*step_size//n_update_iters} AND {step*step_size+i*step_size//n_update_iters}"
                )
                cur.execute("vacuum t")

            # cur.execute("select pg_table_size('t')")
            # logical_size = cur.fetchone()[0]
            logical_size = client.timeline_detail(tenant_id, timeline_id)["current_logical_size"]
            log.info(f"Logical storage size  {logical_size}")
            physical_size = client.timeline_detail(tenant_id, timeline_id)["current_physical_size"]
            log.info(f"Physical storage size {physical_size}")

            client.timeline_checkpoint(tenant_id, timeline_id)

            # Do compaction and GC
            client.timeline_gc(tenant_id, timeline_id, 0)
            client.timeline_compact(tenant_id, timeline_id)

            physical_size = client.timeline_detail(tenant_id, timeline_id)["current_physical_size"]
            log.info(f"Physical after GC     {physical_size}")