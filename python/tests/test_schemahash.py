from ampybus.schemahash import expected_schema_hash, verify_schema_hash

def test_expected_schema_hash_fallback():
    fqdn = "ampy.bars.v1.BarBatch"  # not linked; fallback
    h = expected_schema_hash(fqdn)
    assert h.startswith("nameonly:sha256:")

def test_verify_ok():
    fqdn = "ampy.bars.v1.BarBatch"
    exp = expected_schema_hash(fqdn)
    verify_schema_hash(fqdn, exp)  # should not raise
