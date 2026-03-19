"""
Módulo para exportar DataFrame para Excel com formatação automática
baseada no tipo de tabela.

Detecta automaticamente presets para:
- tabela_descricoes
- tabela_codigos
- tabela_itens_caracteristicas / tab_itens_caract_normalizada
- genérico
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from rich import print as rprint


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

def _is_empty_df(df: Any) -> bool:
    return df.height == 0 if hasattr(df, "height") else df.empty



def _to_pandas(df: Any) -> pd.DataFrame:
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    if isinstance(df, pd.DataFrame):
        return df.copy()
    return pd.DataFrame(df)



def _sanitize_sheet_name(nome: str) -> str:
    proibidos = set(r'[]:*?/\\')
    nome = "".join("_" if c in proibidos else c for c in str(nome))
    nome = nome.strip() or "Dados"
    return nome[:31]



def _serializar_valor(v: Any) -> Any:
    if isinstance(v, (list, tuple, set)):
        itens = [str(x) for x in v if x is not None and str(x) != ""]
        return " | ".join(itens)
    return v



def _serializar_listas(df_pd: pd.DataFrame) -> pd.DataFrame:
    df_pd = df_pd.copy()
    for col in df_pd.columns:
        df_pd[col] = df_pd[col].map(_serializar_valor)
    return df_pd



def _normalizar_objetos(df_pd: pd.DataFrame) -> pd.DataFrame:
    df_pd = df_pd.copy()
    for col in df_pd.columns:
        if pd.api.types.is_object_dtype(df_pd[col]):
            df_pd[col] = df_pd[col].fillna("")
    return df_pd



def _colunas_lower(df_pd: pd.DataFrame) -> set[str]:
    return {str(c).strip().lower() for c in df_pd.columns}


# -----------------------------------------------------------------------------
# Detecção de preset
# -----------------------------------------------------------------------------

def _detectar_preset(nome_base: str, df_pd: pd.DataFrame) -> str:
    nome = str(nome_base).strip().lower()
    cols = _colunas_lower(df_pd)

    if "tabela_descricoes" in nome:
        return "tabela_descricoes"
    if "tabela_codigos" in nome:
        return "tabela_codigos"
    if "tabela_itens_caracteristicas" in nome or "tab_itens_caract" in nome:
        return "tabela_itens_caracteristicas"

    assinatura_descricoes = {
        "descricao",
        "lista_chave_item_individualizado",
        "lista_cod_normalizado",
    }
    if assinatura_descricoes.issubset(cols):
        return "tabela_descricoes"

    assinatura_codigos = {
        "cod_normalizado",
        "lista_descricao",
        "qtd_descr",
    }
    if assinatura_codigos.issubset(cols):
        return "tabela_codigos"

    assinatura_itens = {
        "chave_item_individualizado",
        "codigo",
        "descricao",
        "lista_unidades",
        "fonte",
    }
    if assinatura_itens.issubset(cols):
        return "tabela_itens_caracteristicas"

    return "generico"


# -----------------------------------------------------------------------------
# Presets
# -----------------------------------------------------------------------------

def _obter_preset_config(preset: str) -> dict[str, Any]:
    base = {
        "zoom": 90,
        "default_row": 18,
        "freeze_panes": (1, 0),
        "hide_gridlines": False,
        "texto_forcado": {
            "cnpj", "cpf", "codigo", "cod_normalizado", "ncm", "cest",
            "gtin", "co_sefin_inferido", "chave_item_individualizado"
        },
        "wrap_cols": {"descricao", "descr_compl", "fonte"},
        "larguras_fixas": {},
        "boolean_cols": set(),
        "integer_cols": set(),
        "decimal_cols": set(),
        "highlight_rules": [],
    }

    presets = {
        "tabela_itens_caracteristicas": {
            **base,
            "zoom": 85,
            "default_row": 20,
            "wrap_cols": base["wrap_cols"] | {"lista_unidades"},
            "larguras_fixas": {
                "chave_item_individualizado": 34,
                "codigo": 16,
                "cod_normalizado": 16,
                "descricao": 42,
                "descr_compl": 28,
                "tipo_item": 14,
                "ncm": 12,
                "cest": 12,
                "gtin": 18,
                "lista_unidades": 18,
                "fonte": 14,
                "co_sefin_inferido": 14,
            },
        },
        "tabela_descricoes": {
            **base,
            "zoom": 85,
            "default_row": 22,
            "wrap_cols": base["wrap_cols"] | {
                "lista_chave_item_individualizado", "lista_cod_normalizado",
                "lista_descr_compl", "lista_tipo_item", "lista_ncm", "lista_cest",
                "lista_gtin", "lista_unids", "lista_fonte", "lista_co_sefin_inferido"
            },
            "boolean_cols": {"co_sefin_divergentes"},
            "larguras_fixas": {
                "descricao": 42,
                "lista_chave_item_individualizado": 34,
                "lista_cod_normalizado": 20,
                "lista_descr_compl": 28,
                "lista_tipo_item": 16,
                "lista_ncm": 16,
                "lista_cest": 16,
                "lista_gtin": 20,
                "lista_unids": 18,
                "lista_fonte": 14,
                "lista_co_sefin_inferido": 18,
                "co_sefin_divergentes": 12,
            },
            "highlight_rules": [
                {
                    "type": "boolean_true",
                    "column": "co_sefin_divergentes",
                }
            ],
        },
        "tabela_codigos": {
            **base,
            "zoom": 85,
            "default_row": 22,
            "wrap_cols": base["wrap_cols"] | {
                "lista_chave_item_individualizado", "lista_descricao", "cods_desagregados"
            },
            "integer_cols": {"qtd_descr"},
            "larguras_fixas": {
                "cod_normalizado": 18,
                "lista_chave_item_individualizado": 34,
                "lista_descricao": 42,
                "cods_desagregados": 28,
                "qtd_descr": 10,
            },
            "highlight_rules": [
                {
                    "type": "greater_than",
                    "column": "qtd_descr",
                    "value": 1,
                }
            ],
        },
        "generico": base,
    }
    return presets.get(preset, base)


# -----------------------------------------------------------------------------
# Formatação
# -----------------------------------------------------------------------------

def _criar_formatos(workbook):
    return {
        "padrao": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
        }),
        "cabecalho": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "bold": True,
            "text_wrap": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        }),
        "texto": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
            "num_format": "@",
        }),
        "wrap": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
            "text_wrap": True,
        }),
        "inteiro": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
            "align": "right",
            "num_format": "0",
        }),
        "decimal": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
            "align": "right",
            "num_format": "#,##0.00",
        }),
        "booleano": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "valign": "top",
            "align": "center",
        }),
        "destaque": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "bg_color": "#FFF2CC",
        }),
        "destaque_forte": workbook.add_format({
            "font_name": "Arial",
            "font_size": 8,
            "bg_color": "#FCE4D6",
        }),
    }



def _largura_auto(serie: pd.Series, header: str, minimo: int = 10, maximo: int = 60) -> int:
    try:
        max_len_dados = int(serie.astype(str).map(len).max()) if len(serie) else 0
    except Exception:
        max_len_dados = 0
    largura = max(max_len_dados, len(str(header))) + 2
    largura = min(largura, maximo)
    largura = max(largura, minimo)
    return largura



def _escolher_formato(col_lower: str, dtype: str, cfg: dict[str, Any], formatos: dict[str, Any]):
    if col_lower in cfg["wrap_cols"] or col_lower.startswith("lista_"):
        return formatos["wrap"]
    if (
        col_lower in cfg["texto_forcado"]
        or any(chave in col_lower for chave in ["codigo", "ncm", "cest", "gtin", "chave", "sefin"])
    ):
        return formatos["texto"]
    if col_lower in cfg["boolean_cols"]:
        return formatos["booleano"]
    if col_lower in cfg["integer_cols"]:
        return formatos["inteiro"]
    if col_lower in cfg["decimal_cols"]:
        return formatos["decimal"]
    if "float" in dtype:
        return formatos["decimal"]
    if "int" in dtype:
        return formatos["inteiro"]
    return formatos["padrao"]



def _aplicar_condicional(worksheet, df_pd: pd.DataFrame, cfg: dict[str, Any], formatos: dict[str, Any]):
    if df_pd.empty:
        return

    mapa_cols = {str(c).strip().lower(): i for i, c in enumerate(df_pd.columns)}
    ultima_linha = len(df_pd)
    ultima_coluna = len(df_pd.columns) - 1

    for regra in cfg.get("highlight_rules", []):
        col_lower = str(regra.get("column", "")).lower()
        if col_lower not in mapa_cols:
            continue
        idx = mapa_cols[col_lower]
        letra = chr(ord("A") + idx) if idx < 26 else None
        if letra is None:
            continue

        if regra["type"] == "boolean_true":
            formula = f'=${letra}2=TRUE'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque"],
            })
        elif regra["type"] == "greater_than":
            valor = regra.get("value", 0)
            formula = f'=${letra}2>{valor}'
            worksheet.conditional_format(1, 0, ultima_linha, ultima_coluna, {
                "type": "formula",
                "criteria": formula,
                "format": formatos["destaque_forte"],
            })


# -----------------------------------------------------------------------------
# Função principal
# -----------------------------------------------------------------------------

def exportar_excel(
    df: Any,
    nome_base: str,
    diretorio_saida: Path,
    nome_aba: str | None = None,
    preset: str | None = None,
) -> Path | None:
    """
    Exporta DataFrame para Excel com preset automático de formatação.

    Args:
        df: DataFrame Pandas ou Polars
        nome_base: nome base do arquivo (sem extensão)
        diretorio_saida: pasta de saída
        nome_aba: nome da aba; se omitido, usa o preset detectado
        preset: força um preset específico; se omitido, detecta automaticamente

    Returns:
        Path do arquivo gerado, ou None se o DataFrame estiver vazio.
    """
    if _is_empty_df(df):
        rprint("[yellow]=> Sem resultados. Arquivo Excel não gerado.[/yellow]")
        return None

    diretorio_saida.mkdir(parents=True, exist_ok=True)
    arquivo_excel = diretorio_saida / f"{nome_base}.xlsx"

    df_pd = _to_pandas(df)
    df_pd = _serializar_listas(df_pd)
    df_pd = _normalizar_objetos(df_pd)

    preset_detectado = preset or _detectar_preset(nome_base, df_pd)
    cfg = _obter_preset_config(preset_detectado)
    nome_aba = _sanitize_sheet_name(nome_aba or preset_detectado or "Dados")

    with pd.ExcelWriter(arquivo_excel, engine="xlsxwriter") as writer:
        df_pd.to_excel(writer, sheet_name=nome_aba, index=False)

        workbook = writer.book
        worksheet = writer.sheets[nome_aba]
        formatos = _criar_formatos(workbook)

        if cfg.get("freeze_panes"):
            worksheet.freeze_panes(*cfg["freeze_panes"])

        worksheet.autofilter(0, 0, len(df_pd), len(df_pd.columns) - 1)
        worksheet.set_default_row(cfg.get("default_row", 18))
        worksheet.set_zoom(cfg.get("zoom", 90))

        if cfg.get("hide_gridlines"):
            worksheet.hide_gridlines(2)

        for col_idx, col_name in enumerate(df_pd.columns):
            col_data = df_pd[col_name]
            col_lower = str(col_name).strip().lower()
            dtype_str = str(col_data.dtype).lower()

            # Cabeçalho
            worksheet.write(0, col_idx, col_name, formatos["cabecalho"])

            # Formatação e largura da coluna
            largura = cfg["larguras_fixas"].get(col_lower)
            if largura is None:
                if col_lower in cfg["wrap_cols"] or col_lower.startswith("lista_"):
                    largura = _largura_auto(col_data, col_name, minimo=16, maximo=42)
                elif col_lower in cfg["texto_forcado"]:
                    largura = _largura_auto(col_data, col_name, minimo=12, maximo=28)
                else:
                    largura = _largura_auto(col_data, col_name, minimo=10, maximo=30)

            fmt = _escolher_formato(col_lower, dtype_str, cfg, formatos)
            worksheet.set_column(col_idx, col_idx, largura, fmt)

        _aplicar_condicional(worksheet, df_pd, cfg, formatos)

    rprint(
        f"[green]   => Relatório Excel exportado:[/green] {arquivo_excel.name} "
        f"[cyan](preset: {preset_detectado})[/cyan]"
    )
    return arquivo_excel


__all__ = ["exportar_excel"]
