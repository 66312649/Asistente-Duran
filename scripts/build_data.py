#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convierte las exportaciones del ERP a CSV por centro para la web.

Lee de:
  imports/Base articulos precio.xlsx
  imports/Importacion Stock.xlsx
  imports/Lista proveedores_05092025.xlsx

Soporta XLSX/XLS reales y CSV/TSV con codificaciones raras (utf-16, etc.)
Solo almacenes 1,2,3,4. Mantiene overrides (EAN).
"""

import os, sys, io, json, zipfile, re
import pandas as pd

try:
    import chardet
except Exception:
    chardet = None

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

# ------------- sniffers y lectura robusta ----------------

def sniff_format(path: str) -> str:
    if not os.path.exists(path):
        return "missing"
    try:
        with open(path, "rb") as f:
            head = f.read(8)
        if head.startswith(b"PK\x03\x04"):
            return "xlsx"  # zip (xlsx)
        if head.startswith(b"\xD0\xCF\x11\xE0"):
            return "xls"   # OLE (xls)
    except:
        pass
    return "unknown"

def guess_encoding(b: bytes) -> str:
    # preferimos chardet si está disponible
    if chardet:
        g = chardet.detect(b or b"")
        enc = (g.get("encoding") or "").lower()
        if enc:
            return enc
    # fallback común
    return "utf-8"

def count_candidates(text_head: str, candidates=(';', '\t', ',', '|')) -> str:
    counts = {sep: text_head.count(sep) for sep in candidates}
    best = max(counts, key=counts.get)
    return best

def read_text_with_best_sep(path: str, encodings, candidates=(';', '\t', ',', '|')) -> pd.DataFrame:
    """
    Lee un fichero de texto probando codificaciones y separadores.
    Elige el separador que produzca más columnas (>1). Si todo falla,
    hace un split manual por el más frecuente en cabecera.
    """
    with open(path, "rb") as fb:
        raw = fb.read()

    # detectamos encoding si no nos lo pasan
    enc_guessed = guess_encoding(raw)
    encodings = [enc_guessed] + [e for e in encodings if e != enc_guessed]

    # Recortamos un poco de cabecera para estimar sep
    head_text = ""
    for enc in encodings:
        try:
            head_text = raw[:4096].decode(enc, errors="ignore")
            break
        except Exception:
            continue
    sep_hint = count_candidates(head_text)

    # 1) Intento “normal” con pandas y varios sep/enc
    for enc in encodings:
        # probamos primero el sep “inteligente” y luego el resto
        order = [sep_hint] + [s for s in candidates if s != sep_hint]
        for sep in order:
            try:
                df = pd.read_csv(io.StringIO(raw.decode(enc, errors="ignore")),
                                 sep=sep, engine="python", dtype=str)
                if len(df.columns) > 1:
                    log(f"   leído como texto (enc='{enc}', sep='{sep}') filas={len(df)} cols={list(df.columns)}")
                    return df
            except Exception:
                pass

    # 2) Si llegamos aquí, todo fue 1 columna o error: split manual
    sep = sep_hint
    lines = head_text.splitlines()
    if not lines:
        raise RuntimeError("Archivo de texto vacío o ilegible.")

    # reconvierto todo a str con una codificación que no explote
    text = raw.decode(encodings[0], errors="ignore")
    rows = [ln.split(sep) for ln in text.splitlines() if ln.strip()]
    width = max((len(r) for r in rows), default=0)
    if width <= 1:
        # último intento: prueba otro sep con mayor ocurrencia
        for alt in (';', '\t', ',', '|'):
            if alt == sep: 
                continue
            rows = [ln.split(alt) for ln in text.splitlines() if ln.strip()]
            width = max((len(r) for r in rows), default=0)
            if width > 1:
                sep = alt
                break
    # construyo DataFrame manual
    maxw = max((len(r) for r in rows), default=0)
    norm = [r + ['']*(maxw-len(r)) for r in rows]
    df = pd.DataFrame(norm)
    # primera fila = cabecera
    if len(df) == 0:
        raise RuntimeError("No se pudo parsear el texto en filas/columnas.")
    df.columns = [str(c).strip() for c in df.iloc[0].tolist()]
    df = df.iloc[1:].reset_index(drop=True)
    log(f"   split manual (sep='{sep}') filas={len(df)} cols={list(df.columns)}")
    return df

def read_table(path: str) -> pd.DataFrame:
    kind = sniff_format(path)
    log(f" -> {path} detectado: {kind}")
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

    # Texto: probamos codificaciones típicas
    encs = ["utf-8-sig","utf-8","latin-1","utf-16","utf-16le","utf-16be"]
    return read_text_with_best_sep(path, encs)

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_col(df: pd.DataFrame, key: str):
    aliases = ALIASES.get(key, [])
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}
    for a in aliases:
        if a in cols:
            return a
        if a.lower() in lower:
            return lower[a.lower()]
    lk = key.lower()
    for c in cols:
        if lk in c.lower():
            return c
    return None

def ensure_cols(df: pd.DataFrame, required: list, ctx: str):
    missing = [r for r in required if find_col(df, r) is None]
    if missing:
        raise RuntimeError(f"En '{ctx}' faltan columnas clave: {missing}")
    return True

# ------------- Cargas específicas ----------------

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
    out = out.dropna(subset=["NumeroArticulo"]).copy()
    out["NumeroArticulo"] = out["NumeroArticulo"].astype(str).str.strip()
    # normaliza precio (coma → punto)
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
    def to_int(x):
        try:
            return int(float(str(x).replace(",", ".")))
        except:
            return None
    s["CodigoAlmacen"] = s["CodigoAlmacen"].map(to_int)
    s = s[s["CodigoAlmacen"].isin(VALID_ALMACENES)].copy()
    def to_int_nn(x):
        try:
            v = int(float(str(x).replace(",", ".")))
            return max(v, 0)
        except:
            return 0
    s["Stock"] = s["Stock"].map(to_int_nn)
    s["NumeroArticulo"] = s["NumeroArticulo"].astype(str).str.strip()
    piv = s.pivot_table(
        index="NumeroArticulo", columns="CodigoAlmacen", values="Stock",
        aggfunc="sum", fill_value=0
    )
    piv = piv.rename(columns={k: CENTROS[k][0] for k in piv.columns})
    piv = piv.reset_index()
    return piv

def load_proveedores(path):
    df = read_table(path)
    df = normalize_headers(df)
    c_name = find_col(df, "ProveedorNombre")
    if c_name is None:
        return pd.DataFrame(columns=["CodigoProveedor","NombreProveedor"])
    c_code = find_col(df, "CodigoProveedor")
    if c_code is None:
        c_code = find_col(df, "ReferenciaProveedor")
    if c_code is None:
        out = df[[c_name]].rename(columns={c_name:"NombreProveedor"}).drop_duplicates()
        return out
    out = df[[c_code, c_name]].rename(columns={c_code:"CodigoProveedor", c_name:"NombreProveedor"}).drop_duplicates()
    for c in ["CodigoProveedor","NombreProveedor"]:
        out[c] = out[c].astype(str).str.strip()
    return out

# ------------- Overrides EAN ----------------

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
                    res[slug] = json.load(f)
            else:
                res[slug] = {}
        except Exception:
            res[slug] = {}
    return res

# ------------- Build ----------------

def build():
    base = load_base_precios(IMPORTS["base_precios"])
    log(f"[base] filas: {len(base)} cols={list(base.columns)}")

    stocks = load_stock(IMPORTS["stock"])
    log(f"[stock] filas: {len(stocks)} cols={list(stocks.columns)}")

    provs = load_proveedores(IMPORTS["proveedores"])
    log(f"[proveedores] filas: {len(provs)} cols={list(provs.columns)}")

    m = base.merge(stocks, on="NumeroArticulo", how="left")
    for _, (slug, _) in CENTROS.items():
        if slug not in m.columns:
            m[slug] = 0

    if "CodigoProveedor" in base.columns and "CodigoProveedor" in provs.columns:
        m = m.merge(provs, on="CodigoProveedor", how="left")
    elif "NombreProveedor" in provs.columns and "NombreProveedor" not in m.columns:
        pass

    overrides = load_overrides_ean()
    if "CodigoEAN" not in m.columns:
        m["CodigoEAN"] = ""
    for slug in overrides:
        eandict = overrides[slug] or {}
        if not eandict:
            continue
        mask = m["NumeroArticulo"].isin(eandict.keys())
        m.loc[mask, "CodigoEAN"] = m.loc[mask, "NumeroArticulo"].map(eandict).fillna(m.loc[mask, "CodigoEAN"])

    for alm, (slug, _label) in CENTROS.items():
        out_cols = ["NumeroArticulo","ReferenciaProveedor","Descripcion","CodigoEAN","Precio","Stock"]
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
