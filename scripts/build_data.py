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
        "numeroarticulo", "númeroartículo", "numero_articulo", "articulo", "artículo",
        "codigo", "código", "cod articulo", "cod. articulo", "nº artículo", "nº articulo"
    },
    "ReferenciaProveedor": {
        "referenciaproveedor", "refproveedor", "referencia proveedor", "ref. fabricante",
        "catalogo", "nº catalogo", "nº catálogo", "referencia", "ref fabricante"
    },
    "Descripcion": {
        "descripcion", "descripción", "descripcion articulo", "descripción artículo",
        "articulo descripcion", "nombre", "nombre articulo"
    },
    "CodigoEAN": {
        "codigoean", "ean", "codigo ean", "código ean", "cod. barras", "codigo barras", "código barras"
    },
    "Precio": {
        "1. lista precio de ventas", "precio", "pvp", "precio venta", "precio de venta"
    },
    "NombreProveedor": {
        "nombreproveedor", "proveedor", "nombre proveedor", "razon social proveedor", "razón social proveedor"
    },
    "CodigoProveedor": {
        "codigoproveedor", "cod proveedor", "código proveedor", "id proveedor"
    },
    # Stock
    "CodigoAlmacen": {
        "codigo almacen", "almacen", "almacén", "código almacén", "cod. almacén", "cod almacen"
    },
    "EnStock": {
        "en stock", "stock", "existencias", "cantidad", "disponible", "qty"
    },
}

# --------------------------- Utilidades ---------------------------

def log(msg, verbose):
    if verbose:
        print(msg)

def pick_engine(path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xlsm"):
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    return None  # quizá CSV

def read_table(path: str, verbose=False) -> pd.DataFrame:
    """Lee una tabla Excel o CSV de forma tolerante, devolviendo dataframe con dtype=str."""
    eng = pick_engine(path)
    try:
        if eng:
            df = pd.read_excel(path, sheet_name=0, engine=eng, dtype=str)
            log(f"   leído Excel con {eng}: {path}  filas={len(df)}", verbose)
            return df
        # Fallback CSV con ; o ,
        try:
            df = pd.read_csv(path, sep=";", dtype=str)
            log(f"   leído CSV (;) : {path}  filas={len(df)}", verbose)
            return df
        except Exception:
            df = pd.read_csv(path, dtype=str)
            log(f"   leído CSV (,) : {path}  filas={len(df)}", verbose)
            return df
    except Exception as e:
        raise RuntimeError(f"ERROR leyendo {path} -> {e}")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_col(df: pd.DataFrame, target_key: str) -> str | None:
    """Devuelve el nombre real de la columna que encaja con un alias lógico."""
    wants = ALIAS[target_key]
    # primero intentamos match exacto (ignorando mayúsculas y tildes básicas)
    norm = {c: strip(c) for c in df.columns}
    for col, n in norm.items():
        if n in wants:
            return col
    # fallback: contiene todas las palabras
    for col, n in norm.items():
        for w in wants:
            if w in n:
                return col
    return None

def strip(s: str) -> str:
    t = s.lower().strip()
    t = t.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ü","u").replace("ñ","n")
    t = t.replace(":", "").replace(".", "").replace("-", " ")
    t = " ".join(t.split())
    return t

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
        # a veces vienen "12.0 uds"
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

    # 2) Resolver columnas en cada tabla
    # Base precios
    col_num = find_col(base, "NumeroArticulo")
    col_ref = find_col(base, "ReferenciaProveedor")
    col_desc = find_col(base, "Descripcion")
    col_ean = find_col(base, "CodigoEAN")
    col_precio = find_col(base, "Precio")
    col_nomprov_base = find_col(base, "NombreProveedor")  # si existe
    col_codprov_base = find_col(base, "CodigoProveedor")  # para mapear con 'provs'

    needed = [("NumeroArticulo", col_num), ("ReferenciaProveedor", col_ref),
              ("Descripcion", col_desc), ("Precio", col_precio)]
    missing = [k for (k,v) in needed if v is None]
    if missing:
        raise RuntimeError(f"En 'Base articulos precio.xlsx' faltan columnas clave: {missing}")

    # Proveedores
    col_codprov_prov = find_col(provs, "CodigoProveedor")
    col_nomprov_prov = find_col(provs, "NombreProveedor")

    # Stock
    col_num_s = find_col(stock, "NumeroArticulo") or col_num  # a veces coincide
    col_alm = find_col(stock, "CodigoAlmacen")
    col_stk = find_col(stock, "EnStock")
    needed_s = [("NumeroArticulo", col_num_s), ("CodigoAlmacen", col_alm), ("EnStock", col_stk)]
    miss_s = [k for (k,v) in needed_s if v is None]
    if miss_s:
        raise RuntimeError(f"En 'Importacion Stock.xlsx' faltan columnas clave: {miss_s}")

    # 3) Base de artículos y precios (subset y renombrado)
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
        # Mapeo por código de proveedor
        m = provs[[col_codprov_prov, col_nomprov_prov]].dropna()
        m.columns = ["CodigoProveedor", "NombreProveedor"]
        base_aux = pd.DataFrame({
            "NumeroArticulo": df_base["NumeroArticulo"],
            "CodigoProveedor": base[col_codprov_base].astype(str).str.strip()
        })
        df_base = df_base.merge(base_aux, on="NumeroArticulo", how="left")
        df_base = df_base.merge(m, on="CodigoProveedor", how="left")
        df_base.drop(columns=["CodigoProveedor"], inplace=True)
        df_base["NombreProveedor"] = df_base["NombreProveedor"].fillna("")
    # Normalizar precio a texto
    df_base["Precio"] = df_base["Precio"].fillna("").astype(str).str.replace(",", ".", regex=False)

    # 4) Stock por almacén (filtrar 1..4 y agrupar por NumeroArticulo & Almacén)
    st = stock[[col_num_s, col_alm, col_stk]].dropna()
    st.columns = ["NumeroArticulo", "CodigoAlmacen", "EnStock"]
    st["NumeroArticulo"] = st["NumeroArticulo"].astype(str).str.strip()
    st["CodigoAlmacen"] = st["CodigoAlmacen"].astype(str).str.strip()
    st["EnStock"] = st["EnStock"].map(to_int)

    st = st[st["CodigoAlmacen"].isin(ALMACEN_TO_CENTER.keys())]
    # Sumar por artículo y almacén
    st = st.groupby(["NumeroArticulo", "CodigoAlmacen"], as_index=False)["EnStock"].sum()

    # 5) Cargar EAN maestro existente por centro + overrides
    existing_ean = {}  # center -> {NumeroArticulo: EAN}
    for center in CENTERS:
        path_csv = os.path.join(CENTERS[center]["out_dir"], "Articulos.csv")
        df_prev = safe_read_existing_csv(path_csv)
        if df_prev is not None and "NumeroArticulo" in df_prev.columns:
            m = {}
            col_e_prev = "CodigoEAN" if "CodigoEAN" in df_prev.columns else None
            for _, row in df_prev.iterrows():
                num = str(row.get("NumeroArticulo","")).strip()
                ean_prev = str(row.get(col_e_prev,"")).strip() if col_e_prev else ""
                if num and ean_prev:
                    m[num] = ean_prev
            existing_ean[center] = m
        else:
            existing_ean[center] = {}
        # Overrides
        override = load_overrides_ean(center)
        existing_ean[center].update({k:str(v).strip() for k,v in override.items()})

    # 6) Emitir Articulos.csv por centro
    changed_any = False
    for center, cfg in CENTERS.items():
        alm_code = cfg["almacen"]
        out_dir = cfg["out_dir"]
        os.makedirs(out_dir, exist_ok=True)

        # Stock para este almacén
        st_c = st[st["CodigoAlmacen"] == alm_code][["NumeroArticulo", "EnStock"]]
        st_c = st_c.rename(columns={"EnStock": "Stock"})

        df_out = df_base.merge(st_c, on="NumeroArticulo", how="left")
        df_out["Stock"] = df_out["Stock"].fillna(0).map(to_int)

        # Respetar EAN maestro
        keep = existing_ean.get(center, {})
        if keep:
            ean_new = []
            for _, r in df_out.iterrows():
                num = str(r["NumeroArticulo"])
                e = str(r.get("CodigoEAN","")).strip()
                if num in keep and keep[num]:
                    e = keep[num]
                ean_new.append(e)
            df_out["CodigoEAN"] = ean_new

        # Ordenar por NumeroArticulo si es numérico posible
        try:
            df_out["_sort"] = pd.to_numeric(df_out["NumeroArticulo"], errors="coerce")
            df_out = df_out.sort_values(by=["_sort","NumeroArticulo"])
            df_out.drop(columns=["_sort"], inplace=True)
        except Exception:
            df_out = df_out.sort_values(by=["NumeroArticulo"])

        # Selección final y tipos
        df_out = df_out[[
            "NumeroArticulo", "ReferenciaProveedor", "Descripcion",
            "CodigoEAN", "NombreProveedor", "Precio", "Stock"
        ]].copy()

        df_out["Stock"] = df_out["Stock"].map(int)

        out_path = os.path.join(out_dir, "Articulos.csv")

        # Escribir sólo si cambia (para evitar commits vacíos)
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

    if not changed_any:
        print("Sin cambios en ningún centro.")
    else:
        print("Cambios generados en al menos un centro.")

    print("✅ build_data.py finalizado.")

if __name__ == "__main__":
    main()
