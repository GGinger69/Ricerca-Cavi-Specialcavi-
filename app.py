import re
import json
import hashlib
from pathlib import Path
from io import BytesIO

import streamlit as st
import pandas as pd
import numpy as np

# Lettori Opzionali
try: import pdfplumber; PDF_OK = True
except: PDF_OK = False

try: from docx import Document; DOCX_OK = True
except: DOCX_OK = False

try: from rapidfuzz import fuzz; FUZZ_OK = True
except: FUZZ_OK = False

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

APP_TITLE = "Convertitore Cavi – Specialcavi (V9 Aziendale)"
CACHE_DIR = Path("cache")
CACHE_META = CACHE_DIR / "listino_meta.json"
CACHE_DATA = CACHE_DIR / "listino.parquet"

st.set_page_config(page_title=APP_TITLE, layout="wide")

def sha256_bytes(b: bytes) -> str: return hashlib.sha256(b).hexdigest()

def load_meta():
    try:
        CACHE_DIR.mkdir(exist_ok=True)
        if CACHE_META.exists(): return json.loads(CACHE_META.read_text(encoding="utf-8"))
    except: pass
    return {}

def save_meta(d: dict):
    try:
        CACHE_DIR.mkdir(exist_ok=True)
        CACHE_META.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    except: pass

def norm_text(s: str) -> str:
    if pd.isna(s) or s is None: return ""
    return re.sub(r"\s+", " ", str(s).replace("\u00a0", " ").strip().upper())

def clean_for_family(text: str) -> str:
    t = text.upper()
    t = re.sub(r"FET\s*G", "FTG", t)
    t = re.sub(r"F\s*T\s*G\s*16", "FTG18", t)
    t = re.sub(r"F\s*G\s*16\s*\(?O\)?\s*M\s*16", "FG18OM16", t)
    t = re.sub(r"F\s*T\s*G\s*18\s*\(?O\)?\s*M\s*16", "FTG18OM16", t)
    t = re.sub(r"F\s*G\s*18\s*\(?O\)?\s*M\s*16", "FG18OM16", t)
    t = t.replace("(O)", "O")
    return t

def generate_section_variants(sec_str: str):
    s = sec_str.upper().replace(" ", "").replace(",", ".").replace("+P.E.", "+T").replace("+PE", "+T")
    variants = {s}
    m1 = re.match(r"^(\d+)[X\*](\d+(?:\.\d+)?)\+T$", s)
    if m1: variants.update([f"{int(m1.group(1))+1}G{m1.group(2)}", f"{m1.group(1)}X{m1.group(2)}+T", f"{int(m1.group(1))+1}X{m1.group(2)}"])
    m2 = re.match(r"^(\d+)G(\d+(?:\.\d+)?)$", s)
    if m2:
        variants.update([f"{m2.group(1)}G{m2.group(2)}", f"{m2.group(1)}X{m2.group(2)}"])
        if int(m2.group(1)) > 1: variants.update([f"{int(m2.group(1))-1}X{m2.group(2)}+T"])
    m3 = re.match(r"^(\d+)[X\*](\d+(?:\.\d+)?)$", s)
    if m3:
        variants.update([f"{m3.group(1)}X{m3.group(2)}", f"{m3.group(1)}G{m3.group(2)}"])
        if int(m3.group(1)) > 1: variants.update([f"{int(m3.group(1))-1}X{m3.group(2)}+T"])
    return variants

def extract_all_section_variants(text: str):
    t_clean = text.upper().replace("MMQ", "").replace("MM2", "").replace("MM", "").replace(",", ".").replace("+P.E.", "+T").replace("+PE", "+T")
    t_clean = re.sub(r"\s*([XG\*\+])\s*", r"\1", t_clean)
    found_sections = set()
    for m in re.finditer(r"(?<!\d)(\d+)[X\*]1[X\*](\d+(?:\.\d+)?)(?!\d)", t_clean):
        found_sections.update([f"1X{float(m.group(2)):g}", f"1G{float(m.group(2)):g}"])
        t_clean = t_clean.replace(m.group(0), "")
    for match in re.finditer(r"(?<!\d)(\d+)[XG\*](\d+(?:\.\d+)?)(?:\+T|\+PE)?(?!\d)", t_clean):
        has_t = "+T" in match.group(0) or "G" in match.group(0)
        base = f"{int(match.group(1))}G{float(match.group(2)):g}" if has_t else f"{int(match.group(1))}X{float(match.group(2)):g}"
        if not has_t and "+T" in text.upper(): base += "+T"
        found_sections.update(generate_section_variants(base))
    return found_sections

def extract_code_tokens(text: str):
    t = clean_for_family(text)
    expanded = set()
    for w in re.findall(r"[A-Z0-9\-\_]+", t):
        if len(w) >= 4 and re.search(r"[A-Z]", w) and re.search(r"[0-9]", w):
            expanded.add(w)
            if "FTG16" in w: expanded.add(w.replace("FTG16", "FTG18"))
            if "FG16" in w: expanded.add(w.replace("FG16", "FG18"))
            if "OM16" in w: expanded.add(w.replace("OM16", "M16"))
            if "M16" in w and "OM" not in w: expanded.add(w.replace("M16", "OM16"))
    return expanded

# NUOVO: Estrattore Quantità
def extract_quantity(text: str) -> str:
    # Cerca numeri tra parentesi quadre o tonde [M 7 248.00] o (50)
    m = re.search(r'\[M\s*([\d\.\s\,]+)\]|\(\s*([\d\.\s\,]+)\s*\)', text.upper())
    if m: return (m.group(1) or m.group(2)).replace(' ', '').strip()
    # Cerca numeri affiancati a ML, MT, METRI
    m2 = re.search(r'\b(\d+(?:\.\d+)?)\s*(?:ML|MT|METRI)\b', text.upper())
    if m2: return m2.group(1).strip()
    return ""

def build_listino_from_excel(b: bytes) -> pd.DataFrame:
    bio = BytesIO(b)
    xl = pd.ExcelFile(bio)
    all_rows = []
    for sh in xl.sheet_names:
        sh_up = sh.strip().upper()
        if "CONDIZION" in sh_up or "VENDITA" in sh_up: continue
        try: df_raw = xl.parse(sh, skiprows=9)
        except: continue
        if df_raw.empty: continue
        df_raw.columns = [str(c).strip().upper() for c in df_raw.columns]
        code_col = next((c for c in df_raw.columns if 'CODICE' in c or 'ARTICOLO' in c), None)
        descr_col = next((c for c in df_raw.columns if 'DESCRIZ' in c), None)
        if not code_col or not descr_col: continue
        for _, row in df_raw.iterrows():
            c_val = str(row[code_col]).replace('nan', '').strip()
            d_val = str(row[descr_col]).replace('nan', '').strip()
            if not c_val and not d_val: continue
            d_norm = norm_text(d_val)
            all_rows.append({
                "famiglia_foglio": sh,
                "codice_articolo": c_val,
                "descrizione": d_val,
                "descr_norm": d_norm,
                "tokens_codice": list(extract_code_tokens(c_val + " " + d_norm)),
                "tokens_sezione": list(extract_all_section_variants(d_norm))
            })
    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

def parse_and_filter_requests(raw: str, listino_df: pd.DataFrame):
    lines = [norm_text(x) for x in (raw or "").splitlines()]
    known_terms = set()
    if not listino_df.empty:
        for tk_list in listino_df["tokens_codice"]:
            for tk in tk_list:
                if len(str(tk)) > 3: known_terms.add(str(tk))
    ban_words = [r"\bFTP\b", r"\bUTP\b", r"\bSCALDANTE\b", r"\bAUDIO\b", r"\bFIBRA\b", r"\bCAT\s?5\b", r"\bCAT\s?6\b", r"\bLAN\b", r"\bCOASSIALE\b"]
    rows = []
    last_fams = set()
    for ln in lines:
        if not ln or len(ln) < 3: continue
        if re.fullmatch(r"[0-9\.\,\s\-]+", ln): continue
        if any(re.search(bw, ln) for bw in ban_words): continue
        if ln.startswith(("DA:", "A:", "OGGETTO:", "HTTP", "WWW")): continue
        sec_variants = extract_all_section_variants(ln)
        code_variants = extract_code_tokens(ln)
        if "RESISTENTE AL FUOCO" in ln or "ANTIFIAMMA" in ln:
            code_variants.add("FTG18OM16")
            code_variants.add("FG18OM16")
        if "LSZH" in ln:
            code_variants.add("FG16M16")
            code_variants.add("FG18OM16")
        if code_variants: last_fams = code_variants.copy()
        elif sec_variants and last_fams: code_variants = last_fams.copy()
        if len(sec_variants) > 0 or any(c in known_terms for c in code_variants):
            rows.append({
                "richiesta_raw": ln,
                "quantita": extract_quantity(ln),
                "tokens_sezione": list(sec_variants),
                "tokens_codice": list(code_variants)
            })
    return pd.DataFrame(rows), lines

def match_request_to_listino(listino_df: pd.DataFrame, req_df: pd.DataFrame, threshold: float):
    if listino_df.empty or req_df.empty: return pd.DataFrame(), req_df
    list_records = listino_df.to_dict(orient="records")
    for lr in list_records:
        lr['search_fams'] = set(lr.get('tokens_codice', []))
        lr['search_secs'] = set(lr.get('tokens_sezione', []))
    matches, non = [], []
    for _, r in req_df.iterrows():
        best_score, best = 0.0, None
        for lr in list_records:
            fam_hit = len(set(r["tokens_codice"]) & lr['search_fams']) > 0
            sec_hit = len(set(r["tokens_sezione"]) & lr['search_secs']) > 0
            score = 0.0
            if fam_hit and sec_hit: score = 1.0
            elif sec_hit: score = 0.5
            elif fam_hit: score = 0.4
            if score > 0:
                fzr = fuzz.partial_ratio(str(r["richiesta_raw"]).upper(), str(lr['descr_norm'])) / 100.0 if FUZZ_OK else 0
                score = score + fzr * 0.1 if score == 1.0 else max(score, fzr * 0.9)
            if score > best_score:
                best_score = score
                best = lr
        if best and best_score >= threshold:
            matches.append({
                "Richiesta Originale": r["richiesta_raw"],
                "Quantità (m)": r["quantita"],
                "Codice Articolo": best["codice_articolo"],
                "Descrizione Offerta": best["descrizione"]
            })
        else:
            non.append({
                "Richiesta Originale": r["richiesta_raw"], 
                "Quantità (m)": r["quantita"]
            })
    return pd.DataFrame(matches), pd.DataFrame(non)

def read_request_file(uploaded) -> str:
    name = (uploaded.name or "").lower()
    if name.endswith(".pdf") and PDF_OK:
        t = ""
        with pdfplumber.open(uploaded) as pdf:
            for p in pdf.pages: t += (p.extract_text() or "") + "\n"
        return t
    if name.endswith(".docx") and DOCX_OK: return "\n".join([p.text for p in Document(uploaded).paragraphs])
    if name.endswith(".xlsx"): return "\n".join([str(x) for sh in pd.ExcelFile(uploaded).sheet_names for x in pd.ExcelFile(uploaded).parse(sh, header=None).iloc[:, 0].dropna().tolist()])
    if name.endswith(".csv"): return "\n".join(pd.read_csv(uploaded, header=None).iloc[:, 0].dropna().astype(str).tolist())
    try: return uploaded.read().decode("utf-8")
    except: return uploaded.read().decode("latin-1", errors="ignore")

def to_xlsx_bytes(match_df, non_df):
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        match_df.to_excel(w, index=False, sheet_name="TROVATI")
        non_df.to_excel(w, index=False, sheet_name="NON_DI_NOSTRA_PRODUZIONE")
    return out.getvalue()

def simple_pdf(title, lines):
    out = BytesIO()
    c = canvas.Canvas(out, pagesize=A4)
    x, y = 15*mm, A4[1] - 20*mm
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, title)
    y -= 10*mm
    c.setFont("Helvetica", 10)
    for ln in lines:
        if y < 15*mm:
            c.showPage(); y = A4[1] - 20*mm; c.setFont("Helvetica", 10)
        c.drawString(x, y, ln[:105])
        y -= 6*mm
    c.showPage()
    c.save()
    return out.getvalue()

def build_customer_reply_pdf(non_df):
    lines = []
    if not non_df.empty:
        lines.append("Grazie per la richiesta, provvederemo a inoltrare offerta")
        lines.append("ad eccezione dei seguenti cavi:")
        lines.append("")
        for _, row in non_df.iterrows():
            lines.append(f" - {row['Richiesta Originale'][:105]}")
    else:
        lines.append("Grazie per la richiesta, provvederemo a inoltrare offerta")
        lines.append("per tutti i cavi richiesti.")
    lines.extend(["", "Cordiali saluti"])
    return simple_pdf("Risposta Cliente", lines)

# INTERFACCIA STREAMLIT
st.title(APP_TITLE)
meta = load_meta()

st.markdown("### 1) Carica Listino Specialcavi (XLSX)")
listino_file = st.file_uploader("Carica Listino", type=["xlsx"])
if listino_file:
    b = listino_file.getvalue()
    h = sha256_bytes(b)
    st.info("Indicizzazione Aziendale V9 in corso...")
    df_new = build_listino_from_excel(b)
    df_new.to_parquet(CACHE_DATA, index=False)
    save_meta({"hash": h, "rows": int(len(df_new))})
    st.success(f"Cache V9 aggiornata ✅ Righe listino: {len(df_new)}")
else:
    if CACHE_DATA.exists():
        try: st.success(f"Listino in cache ✅ Righe: {len(pd.read_parquet(CACHE_DATA))}")
        except: st.warning("Cache illeggibile. Ricarica il listino.")
    else: st.warning("Carica il listino per iniziare.")

st.divider()
st.markdown("### 2) Carica Ricerca Cavi")
colA, colB = st.columns([2, 1])
with colA: pasted = st.text_area("Incolla qui la richiesta", height=160, key="pasted_text")
with colB:
    st.write("")
    if st.button("🧹 Cancella testo incollato"):
        st.session_state["pasted_text"] = ""
        st.rerun()

req_file = st.file_uploader("Oppure carica file", type=["pdf", "docx", "xlsx", "csv"])
st.divider()
st.markdown("### 3) Rapporto generale")
threshold = st.slider("Soglia match (Consigliata: 0.65)", 0.40, 0.95, 0.65, 0.01)

if st.button("📌 Analizza Testo / Genera Rapporto"):
    if not CACHE_DATA.exists():
        st.error("Prima carica un elenco valido (sezione 1).")
        st.stop()
    listino_df = pd.read_parquet(CACHE_DATA)
    raw_text = st.session_state.get("pasted_text", "").strip()
    if not raw_text and req_file: raw_text = read_request_file(req_file)
    if not raw_text:
        st.error("Inserisci testo o carica file!")
        st.stop()
        
    req_cavi_df, req_all_df = parse_and_filter_requests(raw_text, listino_df)
    st.info(f"Testo grezzo: {len(req_all_df)} righe | Sopravvissute al Filtro V9: {len(req_cavi_df)} righe")
    
    if req_cavi_df.empty:
        st.warning("Non è stato trovato nessun potenziale cavo.")
    else:
        match_df, non_df = match_request_to_listino(listino_df, req_cavi_df, threshold)
        
        col1, col2 = st.columns(2)
        col1.metric("✅ Trovati (Match)", len(match_df))
        col2.metric("❌ Non Trovati", len(non_df))
        
        st.subheader("✅ INCONTRO")
        st.dataframe(match_df, use_container_width=True)
        st.subheader("❌ NON DI NOSTRA PRODUZIONE")
        st.dataframe(non_df, use_container_width=True)
        
        st.download_button("⬇️ Scarica report XLSX", data=to_xlsx_bytes(match_df, non_df), file_name="REPORT_AZIENDALE.xlsx")
        st.download_button("⬇️ Scarica risposta cliente (PDF)", data=build_customer_reply_pdf(non_df), file_name="RISPOSTA_CLIENTE.pdf")
