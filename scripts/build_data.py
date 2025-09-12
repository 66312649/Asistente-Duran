#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convierte las exportaciones del ERP a CSV por centro para la web.

- Lee:
  imports/Base articulos precio.xlsx           (artículos + precio)
  imports/Importacion Stock.xlsx               (stocks por almacén)
  imports/Lista proveedores_05092025.xlsx      (nombres de proveedor)

- Soporta XLSX/XLS y CSV/TSV (con auto-separador y auto-codificación)
- Solo almacenes 1,2,3,4 -> coll, calvia, alcudia, santanyi
- Mantiene EAN/ubicaciones/fotos de overrides (si existen)
- Genera:
    calvia/Articulos.csv
    coll/Articulos.csv
    alcudia/Articulos.csv
    santanyi/Articulos.csv
"""

import os, sys, io, json, zipfile
import pandas as pd

VERBOSE = "--verbose" in sys.argv

IMPORTS = {
    "base_precios": "imports/Base articulos precio.xlsx",
    "stock":        "imports/Importacion Stock.xlsx",
    "proveedores":  "imports/Lista proveedores_05092025.xlsx",
}

CENTROS = {
    1: ("coll",     "Duran Coll"),
    2: ("calvia",   "Duran Calvià"),
    3: ("alcudia",  "Duran Alcudia"),
    4: ("santanyi", "Duran Santanyí"),
}
VALID_ALMACENES = set(CENTROS.keys())

# === Alias de cabeceras esperadas ===
ALIASES = {
    "NumeroArticulo": [
        "NumeroArticulo","Número Artículo","Nº artículo","Num Articulo","Núm. Artículo",
        "Cod. Articulo","Código Artículo","Articulo","Artículo","Código","Codigo"
    ],
    "ReferenciaProveedor": [
        "ReferenciaProveedor","Referencia Proveedor","Ref. Proveedor","Nº catálogo fabricante",
        "Referencia","Referencia Fabricante","Ref Fabricante","Ref Proveedor"
    ],
    "Descripcion": [
        "Descripcion","Descripción","Nombre artículo","Denominación","Articulo","Artículo"
    ],
    # Precio preferente
    "Precio": [
        "1. Lista Precio de Ventas","PVP","Precio","Precio Venta","Precio con IVA","Precio sin IVA","Tarifa"
    ],
    "CodigoAlmacen": [
        "Codigo almacen","Código almacen","Codigo Almacen","Código Almacén","Almacen","Almacén"
    ],
    "Stock": [
        "en stock","En stock","Stock","Existencias","Cantidad"
    ],
    "ProveedorNombre": [
        "Nombre Proveedor","Proveedor","Nombre proveedor","Razon Social","Razón Social"
    ],
    "CodigoProveedor": [
        "Codigo Proveedor","Código Proveedor","Cod Proveedor","Cod. Proveedor"
    ],
}

def log(msg: str):
    if VERBOSE:
        print(msg)

# ---------- Utilidades de lectura robusta ----------

def sniff_format(path: str) -> str:
    if not os.path.exists(path):
        return "missing"
    try:
        with open(path, "rb") as f:
            head = f.read(8)
        if head.startswith(b"PK\x03\x04"):
            return "xlsx"
        if head.startswith(b"\xD0\xCF\x11\xE0"):  # comp. binario (xls)
            return "xls"
    except:
        pass
    return "unknown"

def read_table(path: str) -> pd.DataFrame:
    """
    Lee Excel/CSV/TSV (varios separadores/codificaciones). Devuelve dtype=str.
    """
    kind = sniff_format(path)
    log(f" -> {path} detectado: {kind}")

    # Excel real
    if kind == "xlsx":
        try:
            return pd.read_excel(path, sheet_name=0, engine="openpyxl", dtype=str)
        except Exception as e:
            log(f"   openpyxl falló: {e}; pruebo como texto…")
    elif kind == "xls":
        try:
            return pd.read_excel(path, sheet_name=0, engine="xlrd", dtype=str)
        except Exception as e:
            log(f"   xlrd falló: {e}; pruebo como texto…")

    # Texto (CSV/TSV) – probamos sep=None (sniffer) + codificaciones
    for enc in ["utf-8-sig","utf-8","latin-1","utf-16","utf-16le","utf-16be"]:
        try:
            df = pd.read_csv(path, sep=None, engine="python", encoding=enc, dtype=str)
            if df is not None and len(df.columns) > 0:
                log(f"   leído como texto (enc='{enc}') filas={len(df)} cols={list(df.columns)}")
                return df
        except Exception as e:
            last = e
    raise RuntimeError(f"No se pudo leer {path} ni como Excel ni como CSV/TSV: {last}")

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_col(df: pd.DataFrame, key: str):
    """Devuelve el nombre de columna en df que coincide con los alias de `key`."""
    aliases = ALIASES.get(key, [])
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}
    for a in aliases:
        if a in cols:
            return a
        if a.lower() in lower:
            return lower[a.lower()]
    # búsqueda laxa por contiene
    lk = key.lower()
    for c in cols:
        if lk in c.lower():
            return c
    return None

# ---------- Ingesta de bases ----------

def ensure_cols(df: pd.DataFrame, required: list, ctx: str):
    missing = [r for r in required if find_col(df, r) is None]
    if missing:
        raise RuntimeError(f"En '{ctx}' faltan columnas clave: {missing}")
    return True

def load_base_precios(path):
    df = read_table(path)
    df = normalize_headers(df)
    ensure_cols(df, ["NumeroArticulo","ReferenciaProveedor","Descripcion","Precio"], "Base articulos precio.xlsx")

    c_num  = find_col(df, "NumeroArticulo")
    c_ref  = find_col(df, "ReferenciaProveedor")
    c_desc = find_col(df, "Descripcion")
    c_prec = find_col(df, "Precio")

    out = df[[c_num, c_ref, c_desc, c_prec]].rename(columns={
        c_num:"NumeroArticulo", c_ref:"ReferenciaProveedor",
        c_desc:"Descripcion",   c_prec:"Precio"
    })
    # limpiar
    out = out.dropna(subset=["NumeroArticulo"]).copy()
    out["NumeroArticulo"] = out["NumeroArticulo"].astype(str).str.strip()
    out["Precio"] = out["Precio"].astype(str).str.replace(",", ".", regex=False)
    return out

def load_stock(path):
    df = read_table(path)
    df = normalize_headers(df)
    ensure_cols(df, ["NumeroArticulo","CodigoAlmacen","Stock"], "Importacion Stock.xlsx")

    c_num   = find_col(df, "NumeroArticulo")
    c_alm   = find_col(df, "CodigoAlmacen")
    c_stock = find_col(df, "Stock")

    s = df[[c_num, c_alm, c_stock]].rename(columns={
        c_num:"NumeroArticulo", c_alm:"CodigoAlmacen", c_stock:"Stock"
    }).dropna(subset=["NumeroArticulo","CodigoAlmacen"])

    # filtra solo 1,2,3,4
    def to_int(x):
        try:
            return int(float(str(x).replace(",", ".")))
        except:
            return None
    s["CodigoAlmacen"] = s["CodigoAlmacen"].map(to_int)
    s = s[s["CodigoAlmacen"].isin(VALID_ALMACENES)].copy()

    # normaliza stock entero no negativo
    def to_int_nn(x):
        try:
            v = int(float(str(x).replace(",", ".")))
            return max(v, 0)
        except:
            return 0
    s["Stock"] = s["Stock"].map(to_int_nn)

    # pivota a columnas por centro
    s["NumeroArticulo"] = s["NumeroArticulo"].astype(str).str.strip()
    piv = s.pivot_table(
        index="NumeroArticulo", columns="CodigoAlmacen", values="Stock",
        aggfunc="sum", fill_value=0
    )
    piv = piv.rename(columns={k: CENTROS[k][0] for k in piv.columns})
    piv = piv.reset_index()
    return piv  # columnas: NumeroArticulo, coll, calvia, alcudia, santanyi (las que existan)

def load_proveedores(path):
    df = read_table(path)
    df = normalize_headers(df)

    # Soportamos (a) cod_prov → nombre, (b) ref_prov → nombre si viniera así
    c_name = find_col(df, "ProveedorNombre")
    if c_name is None:
        return pd.DataFrame(columns=["CodigoProveedor","NombreProveedor"])

    c_code = find_col(df, "CodigoProveedor")
    if c_code is None:
        # intentamos “ReferenciaProveedor” si esta lista lo usa así
        c_code = find_col(df, "ReferenciaProveedor")

    if c_code is None:
        # devolvemos solo el nombre (se usará si encontramos emparejamiento más tarde)
        out = df[[c_name]].rename(columns={c_name:"NombreProveedor"}).drop_duplicates()
        return out

    out = df[[c_code, c_name]].rename(columns={c_code:"CodigoProveedor", c_name:"NombreProveedor"}).drop_duplicates()
    for c in ["CodigoProveedor","NombreProveedor"]:
        out[c] = out[c].astype(str).str.strip()
    return out

# ---------- Overrides locales (no se pisan) ----------

def load_overrides_ean():
    base = "overrides/ean"
    if not os.path.isdir(base):
        return {}
    res = {}
    for _, (slug, _) in CENTROS.items():
        p = os.path.join(base, f"{slug}.json")
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    res[slug] = json.load(f)  # {NumeroArticulo: EAN}
            else:
                res[slug] = {}
        except Exception:
            res[slug] = {}
    return res

# ---------- Construcción de CSV por centro ----------

def build():
    base = load_base_precios(IMPORTS["base_precios"])
    log(f"[base] filas: {len(base)} cols={list(base.columns)}")

    stocks = load_stock(IMPORTS["stock"])
    log(f"[stock] filas: {len(stocks)} cols={list(stocks.columns)}")

    provs = load_proveedores(IMPORTS["proveedores"])
    log(f"[proveedores] filas: {len(provs)} cols={list(provs.columns)}")

    # Merge base + stocks (left)
    m = base.merge(stocks, on="NumeroArticulo", how="left")
    for _, (slug, _) in CENTROS.items():
        if slug not in m.columns:
            m[slug] = 0

    # NombreProveedor (opcional) si traes un código en base (no siempre)
    # Si en “base” hubiera una columna “CodigoProveedor”, se podría usar aquí:
    if "CodigoProveedor" in base.columns and "CodigoProveedor" in provs.columns:
        m = m.merge(provs, on="CodigoProveedor", how="left")
    elif "NombreProveedor" in provs.columns:
        # Si el Excel de proveedores venía solo con nombre, no hacemos merge masivo
        pass

    # Aplica overrides de EAN por centro (sin pisar lo local)
    overrides = load_overrides_ean()  # dict por slug
    # Preparamos columna CodigoEAN si existe en tu base (si no, la creamos)
    if "CodigoEAN" not in m.columns:
        m["CodigoEAN"] = ""

    # Para cada centro, si hay override para ese Nº artículo, úsalo
    for slug in overrides:
        eandict = overrides[slug] or {}
        if not eandict:
            continue
        # mapea por NumeroArticulo
        m.loc[m["NumeroArticulo"].isin(eandict.keys()), "CodigoEAN"] = \
            m.loc[m["NumeroArticulo"].isin(eandict.keys()), "NumeroArticulo"].map(eandict).fillna(m["CodigoEAN"])

    # Genera CSV por centro
    for alm, (slug, _label) in CENTROS.items():
        out_cols = ["NumeroArticulo","ReferenciaProveedor","Descripcion","CodigoEAN","Precio","Stock"]
        # “Stock” = columna del centro
        dfc = m.copy()
        dfc["Stock"] = dfc[slug].fillna(0).astype(int)
        dfc = dfc[out_cols]
        dfc = dfc.sort_values(["Descripcion","NumeroArticulo"])

        dest_dir = slug
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, "Articulos.csv")
        dfc.to_csv(dest, sep=";", index=False, encoding="utf-8")
        log(f"[write] {dest}: {len(dfc)} filas")

def main():
    if VERBOSE:
        print("▶ Iniciando build_data.py")
    build()

if __name__ == "__main__":
    main()
