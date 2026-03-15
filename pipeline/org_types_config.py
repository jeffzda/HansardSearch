#!/usr/bin/env python3
"""
org_types_config.py — Master configuration for all organisation types.

Structure:
    ORG_TYPES: dict[type_key, OrgType]
        type_key       — short identifier used for folder/report naming
        OrgType.label  — human-readable type name
        OrgType.orgs   — list of OrgDef

    OrgDef.folder      — output folder name under case_studies/
    OrgDef.display     — label used in charts
    OrgDef.aliases     — list of case-sensitive substrings to match in body text

Alias curation principles:
  - Full name always included; acronym only if distinctive in Australian politics
  - Avoid 2-3 letter acronyms that are ambiguous (e.g. ABA, BCA, ASU, FSU)
  - Name changes over time included (e.g. MUA → CFMEU merger)
  - Government agencies included where they are treated like advocacy actors
    in parliamentary debate (AHRC, Productivity Commission, RBA)
"""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class OrgDef:
    folder:  str
    display: str
    aliases: list[str]


@dataclass
class OrgType:
    label: str
    orgs:  list[OrgDef]


ORG_TYPES: dict[str, OrgType] = {

    # ── Already extracted — keep in config for report generation ────────────
    "environmental_ngos": OrgType(
        label="Environmental & Conservation NGOs",
        orgs=[
            OrgDef("ACF",                        "ACF",                ["Australian Conservation Foundation", "ACF"]),
            OrgDef("Wilderness_Society",          "Wilderness Society", ["Wilderness Society"]),
            OrgDef("Greenpeace",                  "Greenpeace",         ["Greenpeace"]),
            OrgDef("Climate_Council",             "Climate Council",    ["Climate Council", "Climate Commission"]),
            OrgDef("EDO",                         "EDO",                ["Environmental Defenders Office", "EDO Australia",
                                                                          "EDO NSW", "EDO Qld", "EDO Victoria",
                                                                          "Environmental Defender"]),
            OrgDef("Sea_Shepherd",                "Sea Shepherd",       ["Sea Shepherd"]),
            OrgDef("Friends_of_the_Earth",        "Friends of the Earth", ["Friends of the Earth"]),
            OrgDef("Lock_the_Gate",               "Lock the Gate",      ["Lock the Gate", "LtG Alliance"]),
            OrgDef("Landcare",                    "Landcare",           ["Landcare Australia", "Landcare"]),
            OrgDef("AMCS",                        "AMCS",               ["Australian Marine Conservation Society", "AMCS"]),
            OrgDef("BirdLife",                    "BirdLife",           ["BirdLife Australia", "BirdLife", "Birds Australia",
                                                                          "Royal Australasian Ornithologists Union"]),
            OrgDef("Humane_Society_International","Humane Soc. Intl",   ["Humane Society International", "HSI Australia"]),
            OrgDef("Bob_Brown_Foundation",        "Bob Brown Fdn",      ["Bob Brown Foundation"]),
            OrgDef("WWF",                         "WWF",                ["WWF-Australia", "WWF Australia",
                                                                          "World Wide Fund for Nature",
                                                                          "World Wildlife Fund", "WWF"]),
            OrgDef("AWC",                         "AWC",                ["Australian Wildlife Conservancy"]),
        ],
    ),

    # ── Business & Industry Peak Bodies ─────────────────────────────────────
    "business_industry": OrgType(
        label="Business & Industry Peak Bodies",
        orgs=[
            OrgDef("BCA",           "Business Council",         ["Business Council of Australia", "Business Council"]),
            OrgDef("ACCI",          "ACCI",                     ["Australian Chamber of Commerce and Industry",
                                                                  "Australian Chamber of Commerce", "ACCI"]),
            OrgDef("Minerals_Council","Minerals Council",       ["Minerals Council of Australia", "Minerals Council"]),
            OrgDef("AiGroup",       "Ai Group",                 ["Australian Industry Group", "Ai Group",
                                                                  "AiG", "Australian Industry Group"]),
            OrgDef("APPEA",         "APPEA",                    ["Australian Petroleum Production and Exploration Association",
                                                                  "APPEA"]),
            OrgDef("Master_Builders","Master Builders",         ["Master Builders Australia", "Master Builders"]),
            OrgDef("Property_Council","Property Council",       ["Property Council of Australia", "Property Council"]),
            OrgDef("ABA",           "Aust. Banking Assoc.",     ["Australian Banking Association",
                                                                  "Australian Bankers Association"]),
            OrgDef("Insurance_Council","Insurance Council",     ["Insurance Council of Australia", "Insurance Council"]),
            OrgDef("Pharmacy_Guild","Pharmacy Guild",           ["Pharmacy Guild of Australia", "Pharmacy Guild"]),
            OrgDef("NRA",           "National Retail Assoc.",   ["National Retail Association"]),
            OrgDef("ARA",           "Aust. Retailers Assoc.",   ["Australian Retailers Association"]),
            OrgDef("REIA",          "Real Estate Institute",    ["Real Estate Institute of Australia",
                                                                  "Real Estate Institute"]),
            OrgDef("TTF",           "Tourism & Transport Forum",["Tourism and Transport Forum", "Tourism Transport Forum"]),
            OrgDef("Telco_Assoc",   "Telco / Comms Industry",   ["Communications Alliance", "Australian Mobile Telecommunications Association",
                                                                  "AMTA"]),
        ],
    ),

    # ── Trade Unions & Labour ────────────────────────────────────────────────
    "trade_unions": OrgType(
        label="Trade Unions & Labour",
        orgs=[
            OrgDef("ACTU",   "ACTU",    ["Australian Council of Trade Unions", "ACTU"]),
            OrgDef("CFMEU",  "CFMEU",   ["CFMEU", "Construction Forestry Maritime Mining",
                                          "Construction Forestry Mining Energy",
                                          "Construction, Forestry"]),
            OrgDef("AWU",    "AWU",     ["Australian Workers Union", "AWU"]),
            OrgDef("MUA",    "MUA",     ["Maritime Union of Australia", "MUA"]),
            OrgDef("AEU",    "AEU",     ["Australian Education Union", "AEU"]),
            OrgDef("ANMF",   "ANMF",    ["Australian Nursing and Midwifery Federation", "ANMF",
                                          "Australian Nursing Federation"]),
            OrgDef("TWU",    "TWU",     ["Transport Workers Union", "TWU"]),
            OrgDef("CPSU",   "CPSU",    ["Community and Public Sector Union", "CPSU"]),
            OrgDef("NTEU",   "NTEU",    ["National Tertiary Education Union", "NTEU"]),
            OrgDef("HSU",    "HSU",     ["Health Services Union", "HSU"]),
            OrgDef("AMWU",   "AMWU",    ["Australian Manufacturing Workers Union", "AMWU"]),
            OrgDef("SDA",    "SDA",     ["Shop, Distributive and Allied", "Shop Distributive"]),
            OrgDef("CEPU",   "CEPU",    ["Communications Electrical Plumbing Union", "CEPU"]),
            OrgDef("United_Workers","United Workers Union", ["United Workers Union"]),
            OrgDef("FSU",    "Finance Sector Union", ["Finance Sector Union"]),
        ],
    ),

    # ── Social Services & Welfare ────────────────────────────────────────────
    "social_services": OrgType(
        label="Social Services & Welfare Organisations",
        orgs=[
            OrgDef("ACOSS",          "ACOSS",              ["Australian Council of Social Service", "ACOSS"]),
            OrgDef("St_Vinnies",     "St Vincent de Paul", ["St Vincent de Paul", "Vinnies", "SVDP"]),
            OrgDef("Salvation_Army", "Salvation Army",     ["Salvation Army"]),
            OrgDef("Mission_Australia","Mission Australia",["Mission Australia"]),
            OrgDef("Brotherhood_StL","Brotherhood of St Laurence", ["Brotherhood of St Laurence"]),
            OrgDef("Anglicare",      "Anglicare",          ["Anglicare"]),
            OrgDef("Catholic_Social_Services","Catholic Social Services",["Catholic Social Services Australia",
                                                                           "Catholic Social Services"]),
            OrgDef("National_Shelter","National Shelter",  ["National Shelter"]),
            OrgDef("Homelessness_Australia","Homelessness Australia",["Homelessness Australia"]),
            OrgDef("NCOSS",          "Council of Social Service",["Council of Social Service"]),
            OrgDef("Uniting_Care",   "UnitingCare",        ["UnitingCare", "Uniting Care"]),
            OrgDef("Foodbank",       "Foodbank",           ["Foodbank Australia", "Foodbank"]),
            OrgDef("WACOSS",         "Welfare Rights",     ["Welfare Rights Centre", "National Welfare Rights"]),
        ],
    ),

    # ── Health Organisations ─────────────────────────────────────────────────
    "health_organisations": OrgType(
        label="Health Organisations & Peak Bodies",
        orgs=[
            OrgDef("AMA",            "AMA",                ["Australian Medical Association", "AMA"]),
            OrgDef("Cancer_Council", "Cancer Council",     ["Cancer Council Australia", "Cancer Council"]),
            OrgDef("Heart_Foundation","Heart Foundation",  ["Heart Foundation", "National Heart Foundation"]),
            OrgDef("Diabetes_Australia","Diabetes Australia",["Diabetes Australia"]),
            OrgDef("Mental_Health_Aus","Mental Health Australia",["Mental Health Australia"]),
            OrgDef("RACGP",          "RACGP",              ["Royal Australian College of General Practitioners", "RACGP"]),
            OrgDef("Dementia_Australia","Dementia Australia",["Dementia Australia", "Alzheimer's Australia"]),
            OrgDef("Medicines_Australia","Medicines Australia",["Medicines Australia"]),
            OrgDef("Consumers_Health","Consumers Health Forum",["Consumers Health Forum"]),
            OrgDef("Public_Health_Assoc","Public Health Assoc.",["Public Health Association of Australia",
                                                                  "Public Health Association"]),
            OrgDef("Australian_Dental","Australian Dental Assoc.",["Australian Dental Association"]),
            OrgDef("AIHW",           "AIHW",               ["Australian Institute of Health and Welfare", "AIHW"]),
            OrgDef("Kidney_Health",  "Kidney Health Aus.", ["Kidney Health Australia"]),
            OrgDef("Stroke_Foundation","Stroke Foundation",["Stroke Foundation"]),
        ],
    ),

    # ── Think Tanks & Policy Institutes ─────────────────────────────────────
    "think_tanks": OrgType(
        label="Think Tanks & Policy Institutes",
        orgs=[
            OrgDef("Grattan_Institute","Grattan Institute",["Grattan Institute"]),
            OrgDef("IPA",            "Institute of Public Affairs",["Institute of Public Affairs", "IPA"]),
            OrgDef("Australia_Institute","The Australia Institute",["The Australia Institute", "Australia Institute"]),
            OrgDef("Lowy_Institute", "Lowy Institute",     ["Lowy Institute"]),
            OrgDef("ASPI",           "ASPI",               ["Australian Strategic Policy Institute", "ASPI"]),
            OrgDef("McKell_Institute","McKell Institute",  ["McKell Institute"]),
            OrgDef("Per_Capita",     "Per Capita",         ["Per Capita think tank", "Per Capita Institute"]),
            OrgDef("CIS",            "Centre for Independent Studies",["Centre for Independent Studies"]),
            OrgDef("CEDA",           "CEDA",               ["Committee for Economic Development of Australia", "CEDA"]),
            OrgDef("Productivity_Commission","Productivity Commission",["Productivity Commission"]),
            OrgDef("Parliamentary_Budget_Office","PBO",   ["Parliamentary Budget Office"]),
            OrgDef("AIFS",           "AIFS",               ["Australian Institute of Family Studies", "AIFS"]),
            OrgDef("Blueprint_Institute","Blueprint Institute",["Blueprint Institute"]),
        ],
    ),

    # ── International Bodies ─────────────────────────────────────────────────
    "international_bodies": OrgType(
        label="International Bodies & Multilateral Organisations",
        orgs=[
            OrgDef("United_Nations", "United Nations",     ["United Nations"]),
            OrgDef("IMF",            "IMF",                ["International Monetary Fund", "IMF"]),
            OrgDef("World_Bank",     "World Bank",         ["World Bank"]),
            OrgDef("OECD",           "OECD",               ["OECD", "Organisation for Economic Co-operation and Development",
                                                             "Organization for Economic Co-operation and Development"]),
            OrgDef("WHO",            "WHO",                ["World Health Organization", "World Health Organisation", "WHO"]),
            OrgDef("WTO",            "WTO",                ["World Trade Organization", "World Trade Organisation", "WTO"]),
            OrgDef("IPCC",           "IPCC",               ["Intergovernmental Panel on Climate Change", "IPCC"]),
            OrgDef("UNESCO",         "UNESCO",             ["UNESCO"]),
            OrgDef("UNHCR",          "UNHCR",              ["UNHCR", "UN High Commissioner for Refugees",
                                                             "UN Refugee Agency"]),
            OrgDef("ILO",            "ILO",                ["International Labour Organization",
                                                             "International Labour Organisation", "ILO"]),
            OrgDef("IAEA",           "IAEA",               ["International Atomic Energy Agency", "IAEA"]),
            OrgDef("G20",            "G20",                ["G20", "Group of Twenty"]),
            OrgDef("APEC",           "APEC",               ["APEC", "Asia-Pacific Economic Cooperation"]),
        ],
    ),

    # ── Indigenous Organisations ─────────────────────────────────────────────
    "indigenous_organisations": OrgType(
        label="Indigenous Australian Organisations",
        orgs=[
            OrgDef("ATSIC",          "ATSIC",              ["Aboriginal and Torres Strait Islander Commission", "ATSIC"]),
            OrgDef("NACCHO",         "NACCHO",             ["National Aboriginal Community Controlled Health",
                                                             "NACCHO"]),
            OrgDef("Reconciliation_Australia","Reconciliation Australia",["Reconciliation Australia"]),
            OrgDef("AIATSIS",        "AIATSIS",            ["Australian Institute of Aboriginal and Torres Strait Islander Studies",
                                                             "AIATSIS"]),
            OrgDef("National_Congress","National Congress", ["National Congress of Australia's First Peoples",
                                                              "National Congress of First Peoples"]),
            OrgDef("Land_Councils",  "Land Councils",      ["Land Council", "Central Land Council",
                                                             "Northern Land Council", "Tiwi Land Council"]),
            OrgDef("NIAA",           "NIAA",               ["National Indigenous Australians Agency", "NIAA"]),
            OrgDef("First_Nations_Foundation","First Nations Fdn",["First Nations Foundation"]),
            OrgDef("Uluru_Statement","Uluru Statement",    ["Uluru Statement from the Heart", "Uluru Statement"]),
            OrgDef("NAIDOC",         "NAIDOC",             ["NAIDOC"]),
        ],
    ),

    # ── Human Rights & Legal ─────────────────────────────────────────────────
    "human_rights": OrgType(
        label="Human Rights & Legal Advocacy",
        orgs=[
            OrgDef("Amnesty_International","Amnesty International",["Amnesty International"]),
            OrgDef("Human_Rights_Watch","Human Rights Watch",["Human Rights Watch"]),
            OrgDef("AHRC",           "Aust. Human Rights Commission",["Australian Human Rights Commission",
                                                                       "Human Rights Commission"]),
            OrgDef("Law_Council",    "Law Council",        ["Law Council of Australia", "Law Council"]),
            OrgDef("Refugee_Council","Refugee Council",    ["Refugee Council of Australia", "Refugee Council"]),
            OrgDef("GetUp",          "GetUp",              ["GetUp!"]),
            OrgDef("Liberty_Victoria","Liberty Victoria",  ["Liberty Victoria"]),
            OrgDef("PIAC",           "PIAC",               ["Public Interest Advocacy Centre", "PIAC"]),
            OrgDef("Australian_Lawyers_Alliance","Lawyers Alliance",["Australian Lawyers Alliance"]),
            OrgDef("Oxfam_Australia","Oxfam Australia",   ["Oxfam Australia", "Oxfam"]),
            OrgDef("Save_the_Children","Save the Children",["Save the Children Australia", "Save the Children"]),
            OrgDef("CARE_Australia", "CARE Australia",     ["CARE Australia"]),
        ],
    ),

    # ── Agriculture & Primary Industry ──────────────────────────────────────
    "agriculture": OrgType(
        label="Agriculture & Primary Industry Organisations",
        orgs=[
            OrgDef("NFF",            "National Farmers Federation",["National Farmers Federation", "NFF"]),
            OrgDef("NSW_Farmers",    "NSW Farmers",        ["NSW Farmers"]),
            OrgDef("VFF",            "Victorian Farmers Federation",["Victorian Farmers Federation", "VFF"]),
            OrgDef("Cattle_Council", "Cattle Council",     ["Cattle Council of Australia", "Cattle Council"]),
            OrgDef("Grain_Growers",  "Grain Growers",      ["Grain Growers Australia", "Grain Growers",
                                                             "Grains Council"]),
            OrgDef("AWI",            "Australian Wool Innovation",["Australian Wool Innovation", "AWI"]),
            OrgDef("Cotton_Australia","Cotton Australia",  ["Cotton Australia"]),
            OrgDef("Hort_Innovation","Hort Innovation",   ["Hort Innovation", "Horticulture Australia"]),
            OrgDef("Dairy_Australia","Dairy Australia",    ["Dairy Australia"]),
            OrgDef("Australian_Pork","Australian Pork",    ["Australian Pork Limited", "Australian Pork"]),
            OrgDef("Sugar_Research", "Sugar Research Aus.", ["Sugar Research Australia", "CANEGROWERS"]),
            OrgDef("Agforce",        "AgForce",            ["AgForce Queensland", "AgForce"]),
            OrgDef("GRDC",           "GRDC",               ["Grains Research and Development Corporation",
                                                             "GRDC"]),
        ],
    ),

    # ── Resources & Extractive Industry ─────────────────────────────────────
    "resources_energy": OrgType(
        label="Resources, Mining & Energy Industry Organisations",
        orgs=[
            OrgDef("QRC",            "Queensland Resources Council",["Queensland Resources Council", "QRC"]),
            OrgDef("NSW_Minerals",   "NSW Minerals Council",["NSW Minerals Council"]),
            OrgDef("Coal_Association","Australian Coal Association",["Australian Coal Association"]),
            OrgDef("CME",            "Chamber of Minerals & Energy",["Chamber of Minerals and Energy"]),
            OrgDef("AMEC",           "AMEC",               ["Association of Mining and Exploration Companies", "AMEC"]),
            OrgDef("Clean_Energy_Council","Clean Energy Council",["Clean Energy Council"]),
            OrgDef("Australian_Pipelines","Australian Pipelines",["Australian Pipelines and Gas Association",
                                                                   "Australian Pipelines"]),
            OrgDef("Uranium_Producers","Uranium Industry",  ["Australian Uranium Association",
                                                              "Uranium Information Centre"]),
        ],
    ),

    # ── Religious & Faith-Based ──────────────────────────────────────────────
    "religious_organisations": OrgType(
        label="Religious & Faith-Based Organisations",
        orgs=[
            OrgDef("Australian_Christian_Lobby","Australian Christian Lobby",["Australian Christian Lobby", "ACL"]),
            OrgDef("Catholic_Bishops","Catholic Bishops Conference",["Australian Catholic Bishops Conference",
                                                                      "Catholic Bishops Conference",
                                                                      "Catholic Bishops"]),
            OrgDef("Anglican_Church","Anglican Church",    ["Anglican Church of Australia", "Anglican Church"]),
            OrgDef("Islamic_Council","Islamic Council",    ["Australian Federation of Islamic Councils",
                                                            "Islamic Council of Victoria",
                                                            "Islamic Council"]),
            OrgDef("Uniting_Church", "Uniting Church",    ["Uniting Church in Australia", "Uniting Church"]),
            OrgDef("Jewish_Community","Jewish Community",  ["Executive Council of Australian Jewry",
                                                            "Jewish community in Australia"]),
            OrgDef("National_Council_Churches","National Council of Churches",["National Council of Churches in Australia",
                                                                                "National Council of Churches"]),
            OrgDef("Buddhist_Council","Buddhist Council",  ["Buddhist Council of Australia",
                                                            "Buddhist Council"]),
        ],
    ),

    # ── Media & Communications ───────────────────────────────────────────────
    "media_organisations": OrgType(
        label="Media & Communications Organisations",
        orgs=[
            OrgDef("News_Corp",      "News Corp / News Ltd",["News Corp Australia", "News Limited", "News Ltd"]),
            OrgDef("Nine_Fairfax",   "Nine / Fairfax",      ["Nine Entertainment", "Fairfax Media"]),
            OrgDef("MEAA",           "MEAA",                ["Media Entertainment and Arts Alliance", "MEAA"]),
            OrgDef("Australian_Press_Council","Press Council",["Australian Press Council", "Press Council"]),
            OrgDef("Free_TV",        "Free TV Australia",   ["Free TV Australia", "Free TV"]),
            OrgDef("Community_Broadcasting","Community Broadcasting",["Community Broadcasting Association",
                                                                       "Community Broadcasting Foundation"]),
        ],
    ),
}
