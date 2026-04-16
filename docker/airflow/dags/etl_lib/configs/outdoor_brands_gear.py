"""
Outdoor Brands & Gear Regex Constants
======================================
品牌 + 品類預編譯 regex，供 outdoor_transform 和 pipeline report 共用。
"""

import re

BRAND_PATTERNS: dict[str, re.Pattern] = {
    "Arc'teryx":      re.compile(r"arc.?teryx|始祖鳥|鳥牌", re.I),
    "The North Face": re.compile(r"north\s?face|北臉|TNF", re.I),
    "Mammut":         re.compile(r"mammut|長毛象", re.I),
    "mont-bell":      re.compile(r"mont.?bell", re.I),
    "Salomon":        re.compile(r"salomon", re.I),
    "Merrell":        re.compile(r"merrell", re.I),
    "Columbia":       re.compile(r"columbia|哥倫比亞", re.I),
    "Osprey":         re.compile(r"osprey", re.I),
    "Gregory":        re.compile(r"gregory", re.I),
    "Black Diamond":  re.compile(r"black\s?diamond", re.I),
    "Patagonia":      re.compile(r"patagonia", re.I),
    "HOKA":           re.compile(r"hoka", re.I),
    "Decathlon":      re.compile(r"迪卡儂|decathlon", re.I),
    "Snow Peak":      re.compile(r"snow\s?peak", re.I),
    "Sea to Summit":  re.compile(r"sea\s?to\s?summit", re.I),
    "Nemo":           re.compile(r"nemo", re.I),
    "Lowa":           re.compile(r"lowa", re.I),
    "La Sportiva":    re.compile(r"la\s?sportiva", re.I),
    "Hilleberg":      re.compile(r"hilleberg", re.I),
    "MSR":            re.compile(r"\bMSR\b"),
}

GEAR_PATTERNS: dict[str, re.Pattern] = {
    "登山鞋":     re.compile(r"登山鞋|登山靴|健行鞋|防水鞋|gore.?tex.*鞋", re.I),
    "背包":       re.compile(r"背包|登山包|攻頂包", re.I),
    "外套":       re.compile(r"雨衣|風衣|防水外套|衝鋒衣|軟殼|硬殼", re.I),
    "登山杖":     re.compile(r"登山杖|手杖|trekking.?pole", re.I),
    "底層衣":     re.compile(r"排汗衣|底層衣|機能衣|速乾衣|羊毛衣|美麗諾", re.I),
    "帳篷":       re.compile(r"帳篷|天幕|內帳|外帳", re.I),
    "睡袋/睡墊":  re.compile(r"睡袋|睡墊|充氣墊", re.I),
    "頭燈":       re.compile(r"頭燈", re.I),
    "爐具":       re.compile(r"爐頭|攻頂爐|瓦斯罐|炊具", re.I),
    "保溫瓶":     re.compile(r"保溫瓶|保溫壺", re.I),
    "毛巾":       re.compile(r"快乾毛巾|速乾毛巾|運動毛巾", re.I),
    "露營桌椅":   re.compile(r"露營桌|露營椅|折疊桌|折疊椅", re.I),
}
