"""FIPS 10-4 country codes used by GDELT's ActionGeo_CountryCode field.

Note these are FIPS codes, not ISO: for example GM is Germany, CH is China,
UK is the United Kingdom, and RS is Russia. Unknown codes fall back to the
code itself so nothing ever crashes on an unmapped territory.
"""

FIPS_COUNTRIES = {
    "AF": "Afghanistan", "AL": "Albania", "AG": "Algeria", "AN": "Andorra",
    "AO": "Angola", "AV": "Anguilla", "AC": "Antigua and Barbuda", "AR": "Argentina",
    "AM": "Armenia", "AA": "Aruba", "AS": "Australia", "AU": "Austria",
    "AJ": "Azerbaijan", "BF": "Bahamas", "BA": "Bahrain", "BG": "Bangladesh",
    "BB": "Barbados", "BO": "Belarus", "BE": "Belgium", "BH": "Belize",
    "BN": "Benin", "BD": "Bermuda", "BT": "Bhutan", "BL": "Bolivia",
    "BK": "Bosnia and Herzegovina", "BC": "Botswana", "BR": "Brazil",
    "BX": "Brunei", "BU": "Bulgaria", "UV": "Burkina Faso", "BM": "Myanmar",
    "BY": "Burundi", "CB": "Cambodia", "CM": "Cameroon", "CA": "Canada",
    "CV": "Cape Verde", "CJ": "Cayman Islands", "CT": "Central African Republic",
    "CD": "Chad", "CI": "Chile", "CH": "China", "CO": "Colombia",
    "CN": "Comoros", "CG": "DR Congo", "CF": "Congo", "CS": "Costa Rica",
    "IV": "Ivory Coast", "HR": "Croatia", "CU": "Cuba", "CY": "Cyprus",
    "EZ": "Czechia", "DA": "Denmark", "DJ": "Djibouti", "DO": "Dominica",
    "DR": "Dominican Republic", "EC": "Ecuador", "EG": "Egypt",
    "ES": "El Salvador", "EK": "Equatorial Guinea", "ER": "Eritrea",
    "EN": "Estonia", "WZ": "Eswatini", "ET": "Ethiopia", "FJ": "Fiji",
    "FI": "Finland", "FR": "France", "GB": "Gabon", "GA": "Gambia",
    "GZ": "Gaza Strip", "GG": "Georgia", "GM": "Germany", "GH": "Ghana",
    "GR": "Greece", "GL": "Greenland", "GJ": "Grenada", "GT": "Guatemala",
    "GV": "Guinea", "PU": "Guinea-Bissau", "GY": "Guyana", "HA": "Haiti",
    "HO": "Honduras", "HK": "Hong Kong", "HU": "Hungary", "IC": "Iceland",
    "IN": "India", "ID": "Indonesia", "IR": "Iran", "IZ": "Iraq",
    "EI": "Ireland", "IS": "Israel", "IT": "Italy", "JM": "Jamaica",
    "JA": "Japan", "JO": "Jordan", "KZ": "Kazakhstan", "KE": "Kenya",
    "KR": "Kiribati", "KN": "North Korea", "KS": "South Korea", "KV": "Kosovo",
    "KU": "Kuwait", "KG": "Kyrgyzstan", "LA": "Laos", "LG": "Latvia",
    "LE": "Lebanon", "LT": "Lesotho", "LI": "Liberia", "LY": "Libya",
    "LS": "Liechtenstein", "LH": "Lithuania", "LU": "Luxembourg",
    "MC": "Macau", "MK": "North Macedonia", "MA": "Madagascar", "MI": "Malawi",
    "MY": "Malaysia", "MV": "Maldives", "ML": "Mali", "MT": "Malta",
    "RM": "Marshall Islands", "MR": "Mauritania", "MP": "Mauritius",
    "MX": "Mexico", "FM": "Micronesia", "MD": "Moldova", "MN": "Monaco",
    "MG": "Mongolia", "MJ": "Montenegro", "MH": "Montserrat", "MO": "Morocco",
    "MZ": "Mozambique", "WA": "Namibia", "NR": "Nauru", "NP": "Nepal",
    "NL": "Netherlands", "NZ": "New Zealand", "NU": "Nicaragua", "NG": "Niger",
    "NI": "Nigeria", "NO": "Norway", "MU": "Oman", "PK": "Pakistan",
    "PS": "Palau", "PM": "Panama", "PP": "Papua New Guinea", "PA": "Paraguay",
    "PE": "Peru", "RP": "Philippines", "PL": "Poland", "PO": "Portugal",
    "RQ": "Puerto Rico", "QA": "Qatar", "RO": "Romania", "RS": "Russia",
    "RW": "Rwanda", "SC": "Saint Kitts and Nevis", "ST": "Saint Lucia",
    "VC": "Saint Vincent", "WS": "Samoa", "SM": "San Marino",
    "TP": "Sao Tome and Principe", "SA": "Saudi Arabia", "SG": "Senegal",
    "RI": "Serbia", "SE": "Seychelles", "SL": "Sierra Leone", "SN": "Singapore",
    "LO": "Slovakia", "SI": "Slovenia", "BP": "Solomon Islands", "SO": "Somalia",
    "SF": "South Africa", "OD": "South Sudan", "SP": "Spain", "CE": "Sri Lanka",
    "SU": "Sudan", "NS": "Suriname", "SW": "Sweden", "SZ": "Switzerland",
    "SY": "Syria", "TW": "Taiwan", "TI": "Tajikistan", "TZ": "Tanzania",
    "TH": "Thailand", "TT": "Timor-Leste", "TO": "Togo", "TN": "Tonga",
    "TD": "Trinidad and Tobago", "TS": "Tunisia", "TU": "Turkey",
    "TX": "Turkmenistan", "TV": "Tuvalu", "UG": "Uganda", "UP": "Ukraine",
    "AE": "United Arab Emirates", "UK": "United Kingdom", "US": "United States",
    "UY": "Uruguay", "UZ": "Uzbekistan", "NH": "Vanuatu", "VE": "Venezuela",
    "VM": "Vietnam", "WE": "West Bank", "YM": "Yemen", "ZA": "Zambia",
    "ZI": "Zimbabwe",
}


def country_name(code: str) -> str:
    """Human-readable name for a FIPS code, falling back to the code."""
    return FIPS_COUNTRIES.get(code, code)
