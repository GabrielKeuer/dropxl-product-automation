# VidaXL Shopify Automation

Automatisk daglig sletning (og fremtidig oprettelse) af VidaXL produkter i Shopify via GitHub Actions.

## Oversigt

| Workflow | Kører | Formål |
|----------|-------|--------|
| Daglig Sletning | 04:00 UTC dagligt | Finder og sletter udgåede produkter |
| _(Kommer)_ Daglig Oprettelse | - | Opretter nye produkter |

## Opsætning (Trin for trin)

### 1. Opret GitHub repo

Opret et nyt **privat** repo og push denne kode til det.

### 2. Opret Shopify API adgang

1. Gå til **Shopify Admin → Settings → Apps and sales channels → Develop apps**
2. Klik **Create an app** → giv den navnet f.eks. "VidaXL Automation"
3. Under **Configuration → Admin API access scopes**, tilføj:
   - `read_products`
   - `read_inventory`
4. Klik **Install app**
5. Kopiér **Admin API access token** (vises kun én gang!)

### 3. Tilføj Variables og Secrets i GitHub

Gå til dit repo → **Settings → Secrets and variables → Actions**

**Variables** (tab: Variables):

| Navn | Værdi | Eksempel |
|------|-------|---------|
| `FEED_URL` | VidaXL feed URL | `https://feed.vidaxl.io/api/v1/feeds/download/f05d7105-88c0-45a4-a3a5-f1b48ba55d2a/DK/vidaXL_dk_dropshipping.csv.zip` |
| `SHOPIFY_STORE` | Dit Shopify domæne | `din-butik.myshopify.com` |
| `DELETE_THRESHOLD` | Max antal sletninger uden godkendelse | `1000` |

**Secrets** (tab: Secrets):

| Navn | Værdi |
|------|-------|
| `SHOPIFY_ACCESS_TOKEN` | Access token fra trin 2 |

### 4. Opret Environment for godkendelse

1. Gå til repo → **Settings → Environments**
2. Klik **New environment**
3. Navngiv den: `sletning-godkendelse`
4. Under **Environment protection rules**:
   - Slå **Required reviewers** til
   - Tilføj dig selv som reviewer
5. Gem

### 5. Opsæt Matrixify Scheduled Import

1. Gå til **Matrixify** i Shopify Admin
2. Opret **Scheduled Import**
3. Sæt URL til:
   ```
   https://raw.githubusercontent.com/DIT-BRUGERNAVN/DIT-REPO/main/output/matrixify_delete.csv
   ```
4. Sæt tidspunkt til **kl. 06:00** (så GitHub Actions er færdig)
5. Sæt format til CSV med semikolon som delimiter

> **NB:** Hvis dit repo er privat, skal du bruge en URL med token:
> `https://DIT-GITHUB-TOKEN@raw.githubusercontent.com/DIT-BRUGERNAVN/DIT-REPO/main/output/matrixify_delete.csv`
>
> Opret et Personal Access Token (classic) under GitHub → Settings → Developer settings → Personal access tokens med `repo` scope.

### 6. Test manuelt

1. Gå til repo → **Actions** → **Daglig Produkt Sletning**
2. Klik **Run workflow** → **Run workflow**
3. Tjek at jobbet kører korrekt og outputtet ser rigtigt ud

## Filstruktur

```
├── .github/workflows/
│   └── daily_delete.yml      ← GitHub Actions workflow
├── scripts/
│   └── delete_products.py    ← Slette-script
├── output/
│   └── matrixify_delete.csv  ← Output til Matrixify (auto-opdateres)
├── requirements.txt
└── README.md
```

## Sikkerhed

- **Threshold:** Hvis scriptet vil slette mere end 1.000 produkter, stoppes processen og du modtager en email fra GitHub. Du kan godkende eller afvise direkte fra linket.
- **Tom fil:** Hvis ingen produkter skal slettes, committes en tom CSV — Matrixify importerer ingenting.
- **Privat repo:** Hold repo'et privat for at beskytte API tokens og feed URLs.

## Troubleshooting

- **Workflow fejler med "missing environment variables"** → Tjek at alle variables og secrets er sat korrekt i Settings
- **Shopify API fejler med 401** → Access token er forkert eller app er ikke installeret
- **Feed download fejler** → Tjek at FEED_URL er korrekt og tilgængelig
- **Matrixify importerer ikke** → Tjek at URL'en til raw filen er korrekt, og at GitHub token har repo scope
