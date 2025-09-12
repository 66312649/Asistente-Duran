import sys
import os
from pathlib import Path
import pandas as pd

# === Entradas esperadas ===
BASE_DIR = Path(__file__).resolve().parents[1]  # raíz del repo
IN_DIR = BASE_DIR / "data" / "incoming"

FILE_ARTICULOS = IN_DIR / "Base articulos precio.xlsx"
FILE_STOCK = IN_DIR / "Importacion Stock.xlsx"
FILE_PROV = IN_DIR / "Lista proveedores_05092025.xlsx"

# === Salidas ===
OUT_DIRS = {
    "calvia": BASE_DIR / "calvia",
    "coll": BASE_DIR / "coll",
    "alcudia": BASE_DIR / "alcudia",
    "santanyi": BASE_DIR / "santanyi",
}

# === Mapeo almacén → centro ===
# Solo admitimos los almacenes 1–4 como pediste:
ALMACEN_TO_CENTER = {
    1: "coll",
    2: "calvia",
    3: "alcudia",
    4: "santanyi",
}

# === Utilidades de normalización de cabeceras ===
def norm(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s).strip()
        .lower()
        .replace(" ", "")
        .replace("\t", "")
        .replace(".", "")
        .replace("_", "")
        .replace("-", "")
        .replace("º", "")
        .replace("°", "")
    )

def read_xlsx_required(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] No se encontró el archivo requerido: {path}", file=sys.stderr)
        sys.exit(2)
    return pd.read_excel(path)

def rename_cols(df: pd.DataFrame, mapping_candidates: dict) -> pd.DataFrame:
    """
    mapping_candidates: {dest_key: [posibles nombres en el Excel]}
    Devuelve df con columnas renombradas a las claves 'dest_key' si encuentra coincidencias.
    """
    current = {norm(c): c for c in df.columns}
    rename_map = {}
    for dest, candidates in mapping_candidates.items():
        found = None
        for cand in candidates:
            if norm(cand) in current:
                found = current[norm(cand)]
                break
        if found:
            rename_map[found] = dest
    return df.rename(columns=rename_map)

def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def coerce_int(x):
    try:
        if pd.isna(x):
            return 0
        return int(float(str(x).replace(",", ".").strip()))
    except Exception:
        return 0

def coerce_price(x):
    if pd.isna(x) or x == "":
        return ""
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return ""

def main():
    # 1) Leer Excel base de artículos (precios, EAN, refs, descripciones)
    df_art = read_xlsx_required(FILE_ARTICULOS)
    df_art = rename_cols(
        df_art,
        {
            "NumeroArticulo": ["NumeroArticulo", "Nº articulo", "Numero articulo", "Articulo", "NumArticulo"],
            "ReferenciaProveedor": ["ReferenciaProveedor", "Ref Proveedor", "Referencia prov", "ref"],
            "Descripcion": ["Descripcion", "Descripción", "Desc"],
            "CodigoEAN": ["CodigoEAN", "EAN", "Codigo EAN", "Código EAN", "codigobarras"],
            "ProveedorId": ["CodigoProveedor", "Cod Proveedor", "Proveedor", "IdProveedor", "ProveedorId"],
            # Precio: clave exacta "1. Lista Precio de Ventas" (tolerante)
            "ListaPrecioVentas": ["1. Lista Precio de Ventas", "1 Lista Precio de Ventas", "Lista Precio Ventas", "Tarifa Ventas"],
        },
    )
    # Asegurar columnas base
    df_art = ensure_columns(
        df_art,
        ["NumeroArticulo", "ReferenciaProveedor", "Descripcion", "CodigoEAN", "ProveedorId", "ListaPrecioVentas"],
    )

    # 2) Leer Excel de stock y quedarnos solo con almacenes 1–4
    df_stock = read_xlsx_required(FILE_STOCK)
    df_stock = rename_cols(
        df_stock,
        {
            "NumeroArticulo": ["NumeroArticulo", "Articulo", "NumArticulo", "Código", "Codigo"],
            "Almacen": ["Codigo almacen", "Cod Almacen", "Almacen", "Almacén", "CodAlmacen"],
            "Stock": ["Stock", "Existencias", "En stock", "Cantidad"],
        },
    )
    df_stock = ensure_columns(df_stock, ["NumeroArticulo", "Almacen", "Stock"])

    # Normalizar tipos
    df_stock["Almacen"] = df_stock["Almacen"].apply(coerce_int)
    df_stock["Stock"] = df_stock["Stock"].apply(coerce_int)
    df_stock["NumeroArticulo"] = df_stock["NumeroArticulo"].astype(str).str.strip()

    # Filtrar almacenes válidos 1–4
    df_stock = df_stock[df_stock["Almacen"].isin(ALMACEN_TO_CENTER.keys())].copy()

    # Sumar stock por artículo y almacén
    df_stock_grp = df_stock.groupby(["NumeroArticulo", "Almacen"], as_index=False)["Stock"].sum()

    # 3) Leer Excel de proveedores (mapear nombre proveedor)
    df_prov = read_xlsx_required(FILE_PROV)
    df_prov = rename_cols(
        df_prov,
        {
            "ProveedorId": ["CodigoProveedor", "Cod Proveedor", "Proveedor", "IdProveedor", "ProveedorId", "Codigo"],
            "NombreProveedor": ["NombreProveedor", "Nombre Proveedor", "ProveedorNombre", "Nombre"],
        },
    )
    df_prov = ensure_columns(df_prov, ["ProveedorId", "NombreProveedor"])
    # Limpiar ids a str
    df_prov["ProveedorId"] = df_prov["ProveedorId"].astype(str).str.strip()

    # 4) Normalizar/base artículos
    df_art["NumeroArticulo"] = df_art["NumeroArticulo"].astype(str).str.strip()
    df_art["ProveedorId"] = df_art["ProveedorId"].astype(str).str.strip()

    # Precio: normalizar a float (string si vacío)
    df_art["ListaPrecioVentas"] = df_art["ListaPrecioVentas"].apply(coerce_price)

    # Join nombre proveedor
    df = pd.merge(
        df_art,
        df_prov[["ProveedorId", "NombreProveedor"]],
        on="ProveedorId",
        how="left",
    )

    # Columnas extra requeridas por el frontal (con valores vacíos si no hay)
    for extra in ["ImagenURL", "FichaTecnica", "Manual"]:
        if extra not in df.columns:
            df[extra] = ""

    # 5) Construir un CSV por centro
    # Preparamos un índice por artículo → { centro: stock }
    # Creamos un diccionario NumeroArticulo -> {center_id: stock}
    stock_map = {}
    for _, row in df_stock_grp.iterrows():
        art = str(row["NumeroArticulo"]).strip()
        alm = int(row["Almacen"])
        center = ALMACEN_TO_CENTER.get(alm)
        if center is None:
            continue
        stock_map.setdefault(art, {}).setdefault(center, 0)
        stock_map[art][center] += int(row["Stock"])

    # Asegurar directorios de salida
    for cdir in OUT_DIRS.values():
        os.makedirs(cdir, exist_ok=True)

    # Para cada centro, volcamos todos los artículos con su stock correspondiente (o 0)
    # y columnas en el orden que espera el index.html
    out_cols = [
        "NumeroArticulo",
        "ReferenciaProveedor",
        "Descripcion",
        "CodigoEAN",
        "NombreProveedor",
        "ImagenURL",
        "FichaTecnica",
        "Manual",
        "1. Lista Precio de Ventas",
        "Precio",
        "Stock",
    ]

    # Crear duplicado "1. Lista..." y "Precio" desde ListaPrecioVentas
    df["1. Lista Precio de Ventas"] = df["ListaPrecioVentas"].apply(
        lambda x: ("" if x == "" else f"{x:.2f}")
    )
    df["Precio"] = df["1. Lista Precio de Ventas"]

    # Rellenos mínimos
    df["ReferenciaProveedor"] = df["ReferenciaProveedor"].fillna("").astype(str)
    df["Descripcion"] = df["Descripcion"].fillna("").astype(str)
    df["CodigoEAN"] = df["CodigoEAN"].fillna("").astype(str)
    df["NombreProveedor"] = df["NombreProveedor"].fillna("").astype(str)

    # Volcar por centro
    for center_id, center_dir in OUT_DIRS.items():
        # Calcular stock de ese centro para cada artículo
        stock_list = []
        for _, r in df.iterrows():
            art = str(r["NumeroArticulo"]).strip()
            s = stock_map.get(art, {}).get(center_id, 0)
            stock_list.append(int(s))

        df_out = df.copy()
        df_out["Stock"] = stock_list

        # Orden y selección de columnas
        for c in out_cols:
            if c not in df_out.columns:
                df_out[c] = ""  # por si faltara algo, no romper

        df_out = df_out[out_cols].copy()

        # Guardar como ; y sin índice
        out_path = center_dir / "Articulos.csv"
        df_out.to_csv(out_path, sep=";", index=False)
        print(f"[OK] Escrito: {out_path}")

    print("\n✅ Conversión finalizada sin errores.")

if __name__ == "__main__":
    main()

