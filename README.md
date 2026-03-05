# DropXL Product Automation

Automatisk daglig oprettelse og sletning af VidaXL produkter i Shopify via GitHub Actions + Matrixify.

---

## Dagligt Flow

```
03:10 UTC  →  Sletning kører (finder udgåede vidaXL produkter)
03:20 UTC  →  Oprettelse kører (scraper VidaXL, finder varianter, opretter nye)
~04:00 UTC →  Filer opdateret i repo
XX:XX      →  Matrixify scheduled import henter filer og importerer til Shopify
```

> **NB:** GitHub cron kan forsinkes 10-60 min. Sæt Matrixify import sent nok til at dække dette.
> Tiderne er UTC. Dansk vintertid = UTC+1, sommertid = UTC+2.

---

## GitHub Variables Guide

Alle variabler styres i: **Repo → Settings → Secrets and variables → Actions → Variables tab**

### Toggles (til/fra)

| Variable | Værdier | Default | Beskrivelse |
|----------|---------|---------|-------------|
| `AUTOMATION_ENABLED` | `true` / `false` | `true` | **Master-switch.** Slår BEGGE automations til/fra. Brug denne hvis du vil pause systemet i en periode. |
| `PRODUCT_ORDER` | `newest` / `random` | `newest` | **Rækkefølge for nye produkter.** `newest` = højeste SKU først (nyeste produkter). `random` = tilfældig rækkefølge (spredning over kategorier). |

### Antal og grænser

| Variable | Type | Default | Beskrivelse |
|----------|------|---------|-------------|
| `MAX_PRODUCTS_PER_RUN` | Tal | `999` | Max antal **produktgrupper** per oprettelseskørsel. En produktgruppe = ét produkt med alle dets varianter. |
| `MAX_VARIANTS_PER_RUN` | Tal | `999` | Max antal **SKUs/varianter** per oprettelseskørsel. Scriptet stopper før det næste produkt hvis denne grænse ville overskrides. |
| `DELETE_THRESHOLD` | Tal | `1000` | Hvis sletning vil fjerne flere end dette antal produkter, **pauses** processen og et GitHub Issue oprettes. Du skal godkende ved at kommentere `approved` på issuet. |

### Forbindelser

| Variable / Secret | Type | Beskrivelse |
|-------------------|------|-------------|
| `FEED_URL` | Variable | VidaXL feed URL (ZIP med CSV). |
| `SHOPIFY_STORE` | Variable | Dit Shopify domæne, f.eks. `din-butik.myshopify.com` |
| `SHOPIFY_ACCESS_TOKEN` | **Secret** | Shopify API access token. Opret via Shopify Admin → Settings → Apps → Develop apps. Kræver scopes: `read_products`, `read_inventory`. |

---

## Eksempler på brug

### Pause automation i en uge
1. Gå til Variables
2. Sæt `AUTOMATION_ENABLED` = `false`
3. Når du er klar igen: sæt `AUTOMATION_ENABLED` = `true`

### Opret kun 50 produkter i dag
1. Sæt `MAX_VARIANTS_PER_RUN` = `50`
2. Kør manuelt via Actions → Run workflow
3. Sæt tilbage til `999` bagefter

### Skift til tilfældig rækkefølge
1. Sæt `PRODUCT_ORDER` = `random`
2. Produkter vælges tilfældigt fra alle nye kandidater
3. Skift tilbage: `PRODUCT_ORDER` = `newest`

### Manuel kørsel
1. Gå til Actions → vælg workflow
2. Klik **Run workflow** → **Run workflow**
3. Virker uanset om `AUTOMATION_ENABLED` er true eller false

---

## Filstruktur

```
├── .github/workflows/
│   ├── daily_delete.yml          ← Sletnings-workflow
│   └── daily_create.yml          ← Oprettelses-workflow
├── scripts/
│   ├── delete_products.py        ← Slette-script
│   └── create_products.py        ← Oprettelses-script (med VidaXL scraping)
├── config/
│   └── Kategori_Config.xlsx      ← Prisconfig, kategorier, rum-mapping
├── output/
│   ├── matrixify_delete.csv      ← Slettefil (opdateres dagligt)
│   └── matrixify_create.xlsx     ← Oprettelsesfil (opdateres dagligt)
├── requirements.txt
└── README.md
```

---

## Matrixify URLs

Sæt disse som scheduled imports i Matrixify:

**Sletning:**
```
https://raw.githubusercontent.com/GabrielKeuer/dropxl-product-automation/main/output/matrixify_delete.csv
```

**Oprettelse:**
```
https://raw.githubusercontent.com/GabrielKeuer/dropxl-product-automation/main/output/matrixify_create.xlsx
```

---

## Regler og logik

### Sletning
- Sammenligner VidaXL feed mod Shopify (kun vendor `vidaXL`, case insensitive)
- SKUs i Shopify men IKKE i feed → slettes
- Andre vendors (Sollux, Benuta osv.) røres aldrig
- Smart sletning: hele produktet slettes med `Command: DELETE` hvis alle varianter udgår, ellers kun enkelt-varianter med `Variant Command: DELETE`

### Oprettelse
- Nye produkter = i feed men IKKE i Shopify
- Kandidat-krav: lager ≥ 20, B2B pris > 0, aktiv kategori
- For varianter i en gruppe: lager ≥ 4, B2B pris > 0
- Scraper VidaXL produktside for at finde variant-grupper (farve, størrelse, antal osv.)
- Kalder VidaXL Product-Variation API for at få SKU per variant-kombination
- Merger nye varianter til eksisterende produkter hvis nogle varianter allerede er i Shopify
- Titel renses for option-værdier (case insensitive) og sættes til Title Case

### Priser
- Styres af `config/Kategori_Config.xlsx`
- Markup % ganges på B2B price
- Slutciffer-logik (afrunding til 9'er)
- Sammenligningspris beregnes fra slutpris

---

## Troubleshooting

| Problem | Løsning |
|---------|---------|
| Workflow fejler med "missing environment variables" | Tjek at alle Variables og Secrets er sat i Settings |
| Shopify API fejl 401 | Access token er forkert eller app ikke installeret |
| Feed download fejler | Tjek FEED_URL er korrekt |
| Matrixify import fejler | Tjek URL er korrekt og repo er public |
| Push rejected | Sletning og oprettelse kørte samtidig — `git pull --rebase` er allerede bygget ind |
| For mange sletninger | Threshold-systemet stopper og opretter et Issue — godkend med `approved` |
| Vil ændre kategorier/markup | Opdater `config/Kategori_Config.xlsx` i repo'et |

---

## Opdatering af Kategori_Config

1. Gå til repo → `config/Kategori_Config.xlsx`
2. Klik på filen → Download
3. Redigér i Excel
4. Upload den opdaterede fil (klik "Add file" → "Upload files" i `config/` mappen)
