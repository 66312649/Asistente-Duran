#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera Articulos.csv por centro a partir de:
- imports/Base articulos precio.xlsx
- imports/Importacion Stock.xlsx
- imports/Lista proveedores_05092025.xlsx

Reglas:
- Solo almacenes {1,2,3,4} => {coll, calvia, alcudia, santanyi}
- Columnas salida:
  NumeroArticulo;ReferenciaProveedor;Descripcion;CodigoEAN;NombreProveedor;Precio;Stock
- El EAN maestro es el del repo (*/Articulos.csv) o overrides/ean/<centro>.json si existen.
- Precio: columna "1. Lista Precio de Ventas" (alias tolerantes).
- Stock: entero (sin decimales).
"""

import os
import sys
import json
import math
import argparse
from datetime import datetime
import pandas as pd

# --------------------------- Configuración ---------------------------

IMPORTS = {
    "base_precios": "imports/Base articulos precio.xlsx",
    "stock": "imports/Importacion Stock.xlsx",
    "proveedores": "imports/Lista proveedores_05092025.xlsx",
}

CENTERS = {
    "coll": {"id": "coll", "almacen": "1", "out_dir": "coll"},
    "calvia": {"id": "calvia", "almacen": "2", "out_dir": "calvia"},
    "alcudia": {"id": "alcudia", "almacen": "3", "out_dir": "alcudia"},
    "santanyi": {"id": "santanyi", "almacen": "4", "out_dir": "santanyi"},
}

ALMACEN_TO_CENTER = {
    "1": "coll",
    "2": "calvia",
    "3": "alcudia",
    "4": "santanyi",
}

# Alias de columnas — tolerantes a diferentes nombres
ALIAS = {
    "NumeroArticulo": {
        "numeroarticulo","númeroartículo","numero_articulo","articulo","artículo",
        "codigo","código","cod articulo","cod. articulo","nº artículo","nº articulo"
    },
    "ReferenciaProveedor": {
        "referenciaproveedor","refproveedor","referencia proveedor","ref. fabricante",
        "catalogo","nº catalogo","nº catálogo","referencia","ref fabricante"
    },
    "Descripcion": {
        "descripcion","descripción","descripcion articulo","descripción artículo",
        "articulo descripcion","nombre","nombre articulo"
    },
    "CodigoEAN": {
        "codigoean","ean","codigo ean","código ean","cod. barras","codigo barras","código barras"
    },
    "Precio": {
        "1. lista precio de ventas","precio","pvp","precio venta","precio de venta"
    },
    "NombreProveedor": {
        "nombreproveedor","proveedor","nombre proveedor","razon social proveedor","razón social proveedor"
    },
    "CodigoProveedor": {
        "codigoproveedor","cod proveedor","código proveedor","id proveedor"
    },
    # Stock
    "CodigoAlmacen": {
        "codigo almacen","almacen","almacén","código almacén","cod. almacén","cod almacen"
    },
    "EnStock": {
        "en stock","stock","existencias","cantidad","disponible","qty"
    },
}

# --------------------------- Utilidades ---------------------------

def log(msg, verbose):
    if verbose:
        print(msg)

def sniff_format(path: str) -> str:
    """Detecta por firma binaria el tipo real de fichero."""
    try:
        with open(path, 'rb') as f:
            sig = f.read(8)
        if sig.startswith(b'PK\x03\x04'):
            return 'xlsx'      # Excel OpenXML (zip)
        if sig.startswith(b'\xD0\xCF\x11\xE0'):
            return 'xls'       # Excel OLE2 (binario)
    except Exception:
        pass
    return 'unknown'          # quizá CSV renombrado

def read_table(path: str, verbose=False) -> pd.DataFrame:
    """
    Lee una tabla Excel (xlsx/xls) o CSV/TSV con varios separadores/codificaciones.
    Devuelve dataframe con dtype=str o lanza RuntimeError si no hay forma humana de leerlo.
    """
    kind = sniff_format(path)
    log(f"   → {path} detectado: {kind}", verbose)

    # 1) Excel nativo
    if kind == 'xlsx':
        try:
            return pd.read_excel(path, sheet_name=0, engine='openpyxl', dtype=str)
        except Exception as e:
            log(f"   ⚠️ openpyxl falló ({e}), probando como CSV/TSV…", verbose)
    elif kind == 'xls':
        try:
            return pd.read_excel(path, sheet_name=0, engine='xlrd', dtype=str)
        except Exception as e:
            log(f"   ⚠️ xlrd falló ({e}), probando como CSV/TSV…", verbose)

    # 2) CSV/TSV con varios separadores y codificaciones
    attempts = []
    for sep in [';', ',', '\t']:
        for enc in ['utf-8-sig', 'utf-8', 'latin-1', 'utf-16', 'utf-16le', 'utf-16be']:
            attempts.append((sep, enc))

    last = None
    for sep, enc in attempts:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, engine='python')
            # si no tiene columnas reales, pasa al siguiente intento
            if df is not None and len(df.columns) > 0:
                log(f"   leído como texto (sep='{sep}', enc='{enc}') filas={len(df)}", verbose)
                return df
        except Exception as e:
            last = e

    raise RuntimeError(f"No se pudo leer {path} ni como Excel ni como CSV/TSV: {last}")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def strip(s: str) -> str:
    t = s.lower().strip()
    t = (t.replace("á","a").replace("é","e").replace("í","i")
           .replace("ó","o").replace("ú","u").replace("ü","u").replace("ñ","n"))
    t = t.replace(":", "").replace(".", "").replace("-", " ")
    t = " ".join(t.split())
    return t

def find_col(df: pd.DataFrame, target_key: str) -> str | None:
    wants = ALIAS[target_key]
    norm = {c: strip(c) for c in df.columns}
    # exacto
    for col, n in norm.items():
        if n in wants:
            return col
    # contiene
    for col, n in norm.items():
        if any(w in n for w in wants):
            return col
    return None

def to_int(x) -> int:
    if x is None:
        return 0
    s = str(x).strip().replace(",", ".")
    if s == "" or s.lower() == "nan":
        return 0
    try:
        v = float(s)
        if math.isnan(v):
            return 0
        return int(round(v))
    except Exception:
        num = "".join(ch for ch in s if (ch.isdigit() or ch in ".-"))
        try:
            return int(round(float(num)))
        except Exception:
            return 0

def safe_read_existing_csv(path: str) -> pd.DataFrame | None:
    if not os.path.isfile(path):
        return None
    try:
        return pd.read_csv(path, sep=";", dtype=str)
    except Exception:
        try:
            return pd.read_csv(path, dtype=str)
        except Exception:
            return None

def load_overrides_ean(center_id: str) -> dict:
    path = os.path.join("overrides", "ean", f"{center_id}.json")
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}

# --------------------------- Proceso ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    verbose = args.verbose

    print("▶ Iniciando build_data.py")

    # 1) Leer tablas de imports/
    base = read_table(IMPORTS["base_precios"], verbose=verbose)
    base = normalize_cols(base)
    stock = read_table(IMPORTS["stock"], verbose=verbose)
    stock = normalize_cols(stock)
    provs = read_table(IMPORTS["proveedores"], verbose=verbose)
    provs = normalize_cols(provs)

    # 2) Resolver columnas
    col_num = find_col(base, "NumeroArticulo")
    col_ref = find_col(base, "ReferenciaProveedor")
    col_desc = find_col(base, "Descripcion")
    col_ean = find_col(base, "CodigoEAN")
    col_precio = find_col(base, "Precio")
    col_nomprov_base = find_col(base, "NombreProveedor")
    col_codprov_base = find_col(base, "CodigoProveedor")

    needed = [("NumeroArticulo", col_num), ("ReferenciaProveedor", col_ref),
              ("Descripcion", col_desc), ("Precio", col_precio)]
    missing = [k for (k,v) in needed if v is None]
    if missing:
        raise RuntimeError(f"En 'Base articulos precio.xlsx' faltan columnas clave: {missing}")

    # Proveedores
    col_codprov_prov = find_col(provs, "CodigoProveedor")
    col_nomprov_prov = find_col(provs, "NombreProveedor")

    # Stock
    col_num_s = find_col(stock, "NumeroArticulo") or col_num
    col_alm = find_col(stock, "CodigoAlmacen")
    col_stk = find_col(stock, "EnStock")
    needed_s = [("NumeroArticulo", col_num_s), ("CodigoAlmacen", col_alm), ("EnStock", col_stk)]
    miss_s = [k for (k,v) in needed_s if v is None]
    if miss_s:
        raise RuntimeError(f"En 'Importacion Stock.xlsx' faltan columnas clave: {miss_s}")

    # 3) Base artículos y precios
    df_base = pd.DataFrame({
        "NumeroArticulo": base[col_num].astype(str).str.strip(),
        "ReferenciaProveedor": base[col_ref].astype(str).str.strip() if col_ref else "",
        "Descripcion": base[col_desc].astype(str).str.strip(),
        "CodigoEAN": base[col_ean].astype(str).str.strip() if col_ean else "",
        "Precio": base[col_precio].astype(str).str.strip(),
    })

    # NombreProveedor
    df_base["NombreProveedor"] = ""
    if col_nomprov_base:
        df_base["NombreProveedor"] = base[col_nomprov_base].astype(str).str.strip()
    elif col_codprov_base and col_codprov_prov and col_nomprov_prov:
        m = provs[[col_codprov_prov, col_nomprov_prov]].dropna()
        m.columns = ["CodigoProveedor", "NombreProveedor"]
        aux = pd.DataFrame({
            "NumeroArticulo": df_base["NumeroArticulo"],
            "CodigoProveedor": base[col_codprov_base].astype(str).str.strip()
        })
        df_base = df_base.merge(aux, on="NumeroArticulo", how="left")
        df_base = df_base.merge(m, on="CodigoProveedor", how="left")
        df_base.drop(columns=["CodigoProveedor"], inplace=True)
        df_base["NombreProveedor"] = df_base["NombreProveedor"].fillna("")

    df_base["Precio"] = df_base["Precio"].fillna("").astype(str).str.replace(",", ".", regex=False)

    # 4) Stock por almacén
    st = stock[[col_num_s, col_alm, col_stk]].dropna()
    st.columns = ["NumeroArticulo", "CodigoAlmacen", "EnStock"]
    st["NumeroArticulo"] = st["NumeroArticulo"].astype(str).str.strip()
    st["CodigoAlmacen"] = st["CodigoAlmacen"].astype(str).str.strip()
    st["EnStock"] = st["EnStock"].map(to_int)
    st = st[st["CodigoAlmacen"].isin(ALMACEN_TO_CENTER.keys())]
    st = st.groupby(["NumeroArticulo", "CodigoAlmacen"], as_index=False)["EnStock"].sum()

    # 5) EAN maestro existente + overrides
    existing_ean = {}
    for center in CENTERS:
        path_csv = os.path.join(CENTERS[center]["out_dir"], "Articulos.csv")
        df_prev = safe_read_existing_csv(path_csv)
        existing_ean[center] = {}
        if df_prev is not None and "NumeroArticulo" in df_prev.columns:
            col_e_prev = "CodigoEAN" if "CodigoEAN" in df_prev.columns else None
            for _, row in df_prev.iterrows():
                num = str(row.get("NumeroArticulo","")).strip()
                ean_prev = str(row.get(col_e_prev,"")).strip() if col_e_prev else ""
                if num and ean_prev:
                    existing_ean[center][num] = ean_prev
        # overrides
        override = load_overrides_ean(center)
        existing_ean[center].update({k:str(v).strip() for k,v in override.items()})

    # 6) Emitir Articulos.csv por centro
    changed_any = False
    for center, cfg in CENTERS.items():
        alm_code = cfg["almacen"]
        out_dir = cfg["out_dir"]
        os.makedirs(out_dir, exist_ok=True)

        st_c = st[st["CodigoAlmacen"] == alm_code][["NumeroArticulo", "EnStock"]]
        st_c = st_c.rename(columns={"EnStock": "Stock"})

        df_out = df_base.merge(st_c, on="NumeroArticulo", how="left")
        df_out["Stock"] = df_out["Stock"].fillna(0).map(to_int)

        # Respetar EAN maestro
        keep = existing_ean.get(center, {})
        if keep:
            df_out["CodigoEAN"] = [
                keep.get(str(r["NumeroArticulo"]).strip(), str(r.get("CodigoEAN","")).strip())
                for _, r in df_out.iterrows()
            ]

        # Ordenar
        try:
            df_out["_s"] = pd.to_numeric(df_out["NumeroArticulo"], errors="coerce")
            df_out = df_out.sort_values(by=["_s","NumeroArticulo"]).drop(columns=["_s"])
        except Exception:
            df_out = df_out.sort_values(by=["NumeroArticulo"])

        df_out = df_out[[
            "NumeroArticulo","ReferenciaProveedor","Descripcion",
            "CodigoEAN","NombreProveedor","Precio","Stock"
        ]].copy()
        df_out["Stock"] = df_out["Stock"].map(int)

        out_path = os.path.join(out_dir, "Articulos.csv")
        csv_new = df_out.to_csv(index=False, sep=";", encoding="utf-8")
        csv_old = None
        if os.path.isfile(out_path):
            with open(out_path, "r", encoding="utf-8", errors="ignore") as fh:
                csv_old = fh.read()
        if csv_old != csv_new:
            with open(out_path, "w", encoding="utf-8") as fh:
                fh.write(csv_new)
            changed_any = True
        print(f"✔ {center}: filas={len(df_out)}  escrito={csv_old!=csv_new}")

    print("✅ build_data.py finalizado (cambios={}).".format(changed_any))

if __name__ == "__main__":
    main()
