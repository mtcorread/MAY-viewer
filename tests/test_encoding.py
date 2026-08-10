"""Encoding of user-supplied inputs (CSVs, shapefile attribute tables).

None of this needs a MAY world, so it runs on a fresh clone. The cases are the
ones that only show up on someone else's machine: accented names, a Windows
locale that isn't UTF-8, and the BOM Excel writes.
"""

import json
from pathlib import Path

import pytest

from mayviewer.prep import boundaries, transit

# "Á" is the sharp case: its UTF-8 second byte (0x81) is undefined in cp1252,
# so a locale-default read on a Spanish Windows raises rather than mojibakes.
STOPS = ["Á Coruña", "Málaga María Zambrano", "Alcalá de Henares"]


def _write_csvs(d: Path, encoding: str = "utf-8") -> tuple[Path, Path]:
    coords = d / "coord_mgu.csv"
    coords.write_text(
        "MGU,latitude,longitude\n"
        + "".join(f"M{i:03d},40.{i},-3.{i}\n" for i in range(len(STOPS))),
        encoding=encoding,
    )
    stops = d / "line_stops.csv"
    stops.write_text(
        "line_id,position,node_mgu,name\n"
        + "".join(f"L1,{i},M{i:03d},{nm}\n" for i, nm in enumerate(STOPS)),
        encoding=encoding,
    )
    return stops, coords


def test_transit_csvs_keep_accented_station_names(tmp_path):
    stops, coords = _write_csvs(tmp_path)
    geom = transit.build_geometry(str(stops), str(coords))
    assert [nm for _lon, _lat, nm in geom["L1"]] == STOPS


def test_transit_csvs_tolerate_the_excel_bom(tmp_path):
    """Saving as "CSV UTF-8" in Excel prepends a BOM, which would otherwise
    land in the first column's header and lose the `line_id` key entirely."""
    stops, coords = _write_csvs(tmp_path, encoding="utf-8-sig")
    geom = transit.build_geometry(str(stops), str(coords))
    assert [nm for _lon, _lat, nm in geom["L1"]] == STOPS


def test_boundary_config_is_read_as_utf8(tmp_path):
    """The config names files, and a filename can carry an accent."""
    (tmp_path / "límites.geojson").write_text(
        '{"type":"FeatureCollection","features":[]}', encoding="utf-8")
    cfg = tmp_path / "boundary_config.json"
    cfg.write_text(
        json.dumps({"levels": {"LGU": {"file": "límites.geojson",
                                       "prop": "NOMBRE", "strategy": "code"}}},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    levels = boundaries.load_boundary_config(cfg)
    assert Path(levels["LGU"].file).name == "límites.geojson"


# ── shapefile attribute tables ───────────────────────────────────────────

shapefile = pytest.importorskip("shapefile", reason="needs the geo extra")


def _write_shp(d: Path, stem: str, encoding: str, cpg: str | None) -> Path:
    p = d / f"{stem}.shp"
    w = shapefile.Writer(str(p.with_suffix("")), encoding=encoding)
    w.field("NAME", "C", size=40)
    for i, nm in enumerate(STOPS):
        w.point(float(i), float(i))
        w.record(nm)
    w.close()
    if cpg:
        p.with_suffix(".cpg").write_text(cpg, encoding="ascii")
    return p


@pytest.mark.parametrize("encoding,cpg", [
    ("utf-8", None),        # the modern default
    ("latin-1", None),      # older Spanish/LatAm exports, no sidecar
    ("latin-1", "ISO-8859-1"),
    ("cp1252", "NOT-A-CODEC"),   # sidecar present but unusable
])
def test_dbf_decodes_whatever_the_export_used(tmp_path, encoding, cpg):
    p = _write_shp(tmp_path, "shapes", encoding, cpg)
    sf = boundaries.open_shapefile(p)
    assert [r[0] for r in sf.iterRecords()] == STOPS


def test_a_mislabelled_cpg_does_not_fail_the_build(tmp_path):
    """Sidecars are advisory. One that claims UTF-8 over a latin-1 table is a
    common export bug and should fall through, not stop a build."""
    p = _write_shp(tmp_path, "shapes", "latin-1", "UTF-8")
    sf = boundaries.open_shapefile(p)
    assert [r[0] for r in sf.iterRecords()] == STOPS


def test_a_corrupt_file_is_not_reported_as_an_encoding_problem(tmp_path):
    """Every 8-bit fallback maps every byte, so a file that still won't read is
    broken for some other reason. Let pyshp's own error through rather than
    burying it under 'tried these four encodings'."""
    p = _write_shp(tmp_path, "shapes", "utf-8", None)
    p.with_suffix(".dbf").write_bytes(b"not a dbf at all")
    with pytest.raises(Exception) as e:
        boundaries.open_shapefile(p)
    assert not isinstance(e.value, SystemExit)
