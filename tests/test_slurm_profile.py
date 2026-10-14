"""slurm_db_export.profile: the profiles against Slurm's own DDL, and detection."""
import pytest

from ear_db_export.config import ConfigError
from slurm_db_export.profile import available_profiles, detect, load_profile, release

from slurmfake import ddl


def columns(rel, table):
    """The column names of ``{cluster}_<table>_table`` in a release's DDL."""
    return {c for c, _, _, _ in ddl(rel)["{cluster}_%s_table" % table]}


@pytest.mark.parametrize("name, oldest, newest", [("slurm17", "17.11", "17.11"),
                                                  ("slurm20", "20.02", "25.05")])
def test_profile_matches_slurm_ddl(name, oldest, newest):
    """Every required column is in the oldest release of the range; every column in the newest, but optional ones."""
    p = load_profile(name)
    for table in ("job", "step"):
        for rel in (oldest, newest):
            names, missing = p.select(table, columns(rel, table))  # raises if a required one is missing
            assert all(p.is_optional(table, c) for c in missing)


def test_slurm20_has_every_column_in_25_05():
    """Nothing the export asks for is missing from today's schema."""
    p = load_profile("slurm20")
    for table in ("job", "step"):
        assert p.select(table, columns("25.05", table))[1] == []


def test_slurm17_renames_pack_job():
    """17.11 stores het_job_id as pack_job_id."""
    p = load_profile("slurm17")
    names, missing = p.select("job", columns("17.11", "job"))
    assert "pack_job_id" in names and "het_job_id" not in names
    assert set(missing) == {"constraints", "flags", "state_reason_prev", "submit_line",
                            "script_hash_inx"}


def test_required_column_missing_is_refused():
    """A database without time_submit does not match."""
    with pytest.raises(ConfigError, match="time_submit"):
        load_profile("slurm20").select("job", columns("25.05", "job") - {"time_submit"})


@pytest.mark.parametrize("version, name", [(5, "slurm17"), (7, "slurm17"), (8, "slurm20"),
                                           (16, "slurm20"), (99, "slurm20")])
def test_detect_by_version(version, name):
    """By convert_version_table; a newer version takes the newest profile."""
    assert detect(version, set()).name == name


def test_detect_by_columns_and_refusals():
    """Without a version, by the het/pack column; an old version or no clue is refused."""
    assert detect(None, columns("17.11", "job")).name == "slurm17"
    assert detect(None, columns("25.05", "job")).name == "slurm20"
    with pytest.raises(ConfigError):
        detect(3, set())
    with pytest.raises(ConfigError):
        detect(None, set())


def test_release_estimate():
    """Shared schema versions are told apart by a column."""
    assert release(16, columns("25.05", "job")) == "25.05 or later"
    assert release(16, columns("25.05", "job") - {"resv_req"}) == "24.11"
    assert release(8, set()) == "20.02" and release(None, set()) is None


def test_profiles_load(tmp_path):
    """Both bundled profiles are valid; one naming an unknown column is refused."""
    assert available_profiles() == ["slurm17", "slurm20"]
    bad = tmp_path / "bad.toml"
    bad.write_text('convert = [1, 2]\n[renames.job]\nno_such = "x"\n')
    with pytest.raises(ConfigError, match="no_such"):
        load_profile(str(bad))
