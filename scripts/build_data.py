#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera /<centro>/Articulos.csv a partir de:
  - imports/base_articulos.csv
  - imports/stock_por_almacen.csv
y aplica overrides opcionales:
  - overrides/ean/<centro>.json      (NumeroArticulo -> CodigoEAN)
  - overrides/images/<centro>.json   (NumeroArticulo -> ImagenURL)

Reglas importantes:
- Solo almacenes {1: coll, 2: calvia, 3: alcudia, 4: santanyi}
- Precio = columna "1. Lista Precio de Ventas" (mapeada abajo)
- EAN/Foto/Ubicación desde tablet/móvil (overrides) SIEMPRE tienen prioridad.
- Ubicaciones (/<centro>/ubicaciones.csv) NO se tocan aquí.
"""

import argparse
import json
import os
from pathlib import Path

import pandas as pd

# ----------- Config -----------

ROOT = Path(__file__).resolve().parents[1]

IMPORTS = {
    "base_precios": ROOT / "imports" / "base_articulos.csv",
    "stock": ROOT / "imports" / "stock_por_almacen.csv",
    # Proveedores opcional (no imprescindible)
    "proveedores": ROOT / "imports" / "lista_proveedores.csv",
}

CENTERS = {
    1: ("coll", "Coll"),
    2: ("calvia", "Calvià"),
    3: ("alcudia", "Alcudia"),
    4: ("santanyi", "Santanyí"),
}

# mapeos tolerantes de nombres de columnas
MAP_BASE = {
    "numero":  ["NumeroArticulo","Nº Articulo","NumArticulo","Articulo","CodigoArticulo","Cód. Articulo"],
    "refprov": ["ReferenciaProveedor","Ref. Prov.","Referencia Prov","REF_PROV","Referencia"],
    "descr":   ["Descripcion","Descripción","Desc","NombreArticulo","Nombre"],
    "precio":  ["1. Lista Precio de Ventas","PrecioLista1","Precio Lista 1","PVP","Precio"],
    "ean":     ["CodigoEAN","EAN","EAN13","Codigo EAN"],
    "prov":    ["NombreProveedor","Proveedor","CodProveedor","ProveedorNombre"],
}

MAP_STOCK = {
    "numero":  ["NumeroArticulo","Articulo","Nº Articulo","CodigoArticulo"],
    "almacen": ["Codigo_almacen","Almacen","CodAlmacen","Almacén","IDAlmacen"],
    "stock":   ["Stock","Existencias","Cantidad","Qty","Unidades"],
}

EXPORT_COLUMNS = [
    "NumeroArticulo",
    "ReferenciaProveedor",
    "Descripcion",
    "CodigoEAN",
    "NombreProveedor",
    "ImagenURL",
    "Precio",
    "Stock",
]

# ----------- Utils -----------

def log(msg):
    print(msg, flush=True)

def find_first(df_cols, candidates):
    """Devuelve el primer nombre de columna existente (case-insensitive)"""
    cols_lower = {c.lower(): c for c in df_cols}
    for name in candidates:
        low = name.lower()
        if low in cols_lower:
            return cols_lower[low]
    return None

def read_csv_smart(path: Path) -> pd.DataFrame:
    """Lee CSV probando separadores y encodings más comunes."""
    tries = [
        {"sep": ";", "encoding": "utf-8-sig"},
        {"sep": ",", "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "latin1"},
        {"sep": ",", "encoding": "latin1"},
    ]
    last_err = None
    for t in tries:
        try:
            df = pd.read_csv(path, sep=t["sep"], encoding=t["encoding"])
            if df.empty or len(df.columns) == 1:
                # puede ser separador incorrecto, seguimos probando
                last_err = RuntimeError("posible separador incorrecto")
                continue
            return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No se pudo leer {path.name} como CSV: {last_err}")

def to_numeric(val):
    """Convierte precios/stock con comas/puntos a número."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    # eliminar separadores de miles y normalizar decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_json_if_exists(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            log(f"⚠️  No se pudo leer JSON: {path}")
    return {}

# ----------- Carga base y stock -----------

def load_base_precios(path: Path, verbose: bool) -> pd.DataFrame:
    if verbose:
        log(f"→ Leyendo base artículos: {path}")
    df = read_csv_smart(path)

    # localizar columnas
    col_num   = find_first(df.columns, MAP_BASE["numero"])
    col_ref   = find_first(df.columns, MAP_BASE["refprov"])
    col_desc  = find_first(df.columns, MAP_BASE["descr"])
    col_prec  = find_first(df.columns, MAP_BASE["precio"])
    col_ean   = find_first(df.columns, MAP_BASE["ean"]) or "CodigoEAN"
    col_prov  = find_first(df.columns, MAP_BASE["prov"]) or "NombreProveedor"

    missing = [("NumeroArticulo", col_num), ("ReferenciaProveedor", col_ref), ("Descripcion", col_desc), ("Precio", col_prec)]
    missing_names = [exp for exp, real in missing if real is None]
    if missing_names:
        raise RuntimeError(f"En {path.name} faltan columnas clave: {missing_names}")

    out = pd.DataFrame()
    out["NumeroArticulo"] = df[col_num].astype(str).str.strip()
    out["ReferenciaProveedor"] = df[col_ref].astype(str).str.strip()
    out["Descripcion"] = df[col_desc].astype(str).str.strip()

    precios = df[col_prec].apply(to_numeric)
    out["Precio"] = precios.fillna(0.0).round(2)

    if col_ean in df.columns:
        out["CodigoEAN"] = df[col_ean].astype(str).str.strip()
    else:
        out["CodigoEAN"] = ""

    if col_prov in df.columns:
        out["NombreProveedor"] = df[col_prov].astype(str).str.strip()
    else:
        out["NombreProveedor"] = ""

    out["ImagenURL"] = ""  # se podrá sobrescribir por override
    if verbose:
        log(f"   · Artículos base: {len(out):,}")
    return out

def load_stock(path: Path, verbose: bool) -> pd.DataFrame:
    if verbose:
        log(f"→ Leyendo stock por almacén: {path}")
    df = read_csv_smart(path)

    col_num   = find_first(df.columns, MAP_STOCK["numero"])
    col_alm   = find_first(df.columns, MAP_STOCK["almacen"])
    col_stock = find_first(df.columns, MAP_STOCK["stock"])

    missing = [("NumeroArticulo", col_num), ("Codigo_almacen", col_alm), ("Stock", col_stock)]
    missing_names = [exp for exp, real in missing if real is None]
    if missing_names:
        raise RuntimeError(f"En {path.name} faltan columnas: {missing_names}")

    tmp = pd.DataFrame()
    tmp["NumeroArticulo"] = df[col_num].astype(str).str.strip()
    tmp["Codigo_almacen"] = pd.to_numeric(df[col_alm], errors="coerce").astype("Int64")
    tmp["Stock"] = df[col_stock].apply(to_numeric).fillna(0)

    # quedarnos solo con almacenes 1..4
    tmp = tmp[tmp["Codigo_almacen"].isin(CENTERS.keys())].copy()

    # agrupar por articulo y almacén
    g = tmp.groupby(["NumeroArticulo", "Codigo_almacen"], as_index=False)["Stock"].sum()

    # pivot a columnas por centro
    pivot = g.pivot(index="NumeroArticulo", columns="Codigo_almacen", values="Stock").fillna(0.0)

    # nombres finales de columnas
    rename_cols = {}
    for cod, (key, _) in CENTERS.items():
        if cod in pivot.columns:
            rename_cols[cod] = f"stock_{key}"
    pivot = pivot.rename(columns=rename_cols)

    # redondear y a int
    for cod, (key, _) in CENTERS.items():
        col = f"stock_{key}"
        if col in pivot.columns:
            pivot[col] = pivot[col].round().astype(int)

    pivot = pivot.reset_index()
    if verbose:
        log(f"   · Registros de stock (tras pivot): {len(pivot):,}")
    return pivot

# ----------- Build -----------

def apply_overrides_per_center(df_out: pd.DataFrame, center_key: str):
    """Aplica overrides de EAN e imagen para un centro concreto."""
    ean_path = ROOT / "overrides" / "ean" / f"{center_key}.json"
    img_path = ROOT / "overrides" / "images" / f"{center_key}.json"

    eans = load_json_if_exists(ean_path)     # {NumeroArticulo: EAN}
    imgs = load_json_if_exists(img_path)     # {NumeroArticulo: URL}

    if eans:
        # Solo aplicar si hay valor (no sobreescribir con vacío)
        df_out["CodigoEAN"] = df_out.apply(
            lambda r: eans.get(str(r["NumeroArticulo"]), r["CodigoEAN"]) or r["CodigoEAN"], axis=1
        )
    if imgs:
        df_out["ImagenURL"] = df_out.apply(
            lambda r: imgs.get(str(r["NumeroArticulo"]), r["ImagenURL"]) or r["ImagenURL"], axis=1
        )
    return df_out

def build(verbose: bool = False):
    base = load_base_precios(IMPORTS["base_precios"], verbose=verbose)
    stock = load_stock(IMPORTS["stock"], verbose=verbose)

    # merge base + stock
    master = base.merge(stock, on="NumeroArticulo", how="left")

    # asegurar columnas de stock aun si no existen
    for cod, (key, _) in CENTERS.items():
        col = f"stock_{key}"
        if col not in master.columns:
            master[col] = 0

    if verbose:
        log(f"→ Master total artículos: {len(master):,}")

    # export por centro
    for cod, (key, label) in CENTERS.items():
        out_dir = ROOT / key
        ensure_dir(out_dir)
        out_path = out_dir / "Articulos.csv"

        dfc = master.copy()
        dfc["Stock"] = dfc[f"stock_{key}"].fillna(0).astype(int)

        # columnas finales
        out = dfc.reindex(columns=[
            "NumeroArticulo",
            "ReferenciaProveedor",
            "Descripcion",
            "CodigoEAN",
            "NombreProveedor",
            "ImagenURL",
            "Precio",
            "Stock",
        ])

        # aplicar overrides (EAN/Imagen)
        out = apply_overrides_per_center(out, key)

        # guardar (; como separador)
        out.to_csv(out_path, sep=";", index=False, encoding="utf-8")

        if verbose:
            log(f"   · [{key}] {len(out):,} artículos -> {out_path.relative_to(ROOT)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    if args.verbose:
        log("▶ Iniciando build_data.py")

    for k, v in IMPORTS.items():
        if k in ("proveedores",) and not v.exists():
            continue
        if not v.exists():
            raise SystemExit(f"ERROR: No existe {v}")

    build(verbose=args.verbose)

    if args.verbose:
        log("✅ Finalizado")

if __name__ == "__main__":
    main()
