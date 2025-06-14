from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA = "data"
AUTH = "config/auth"

AUTH_TWITTER = BASE_DIR / "config" / "auth" / "twitter_auth.json"

repo_name = "tweets-repo"
branch_name = "main"
path = "tweets.parquet"

repo_name_hash = "tweets-hash-repo"
hash_path = "hash_partitioned"

lakefs_s3_path = f"s3://{repo_name}/{branch_name}/{path}"
lakefs_s3_path_hash = f"s3://{repo_name_hash}/{branch_name}/{hash_path}"


tags = {
    "ธรรมศาสตร์": [
         "#ธรรมศาสตร์ช้างเผือก",
         "#TCAS",
         "#รับตรง",
         "#ทีมมธ",
         "#มธ", 
         "#dek70", 
         "#มอท่อ",
         "#TU89",
        "TU lang:th"
    ],
    "คณะนิติศาสตร์":[
        "#นิติศาสตร์",
        "#LawTU",
        "#TUlaw",
        "#นิติมธ",
    ],
    "คณะพาณิชยศาสตร์และการบัญชี":[
        "#บัญชีมธ",
        "#บริหารมธ",
        "#BBATU",
    ],
    # "คณะรัฐศาสตร์":[
    #     "#รัฐศาสตร์มธ",
    #     "#LLBTU",
    #     "#BIRTU",
    #     "#singhadang",
    #     "#สิงห์แดง",
    # ],
    "คณะเศรษฐศาสตร์":[
        "#เสดสาดมธ",
        "#EconTU",
    ],
    # "คณะสังคมสงเคราะห์ศาสตร์":[
    #     "#สังคมสงเคราะห์มธ",
    # ],
    # "คณะสังคมวิทยาและมานุษยวิทยา":[
    #     "#สังวิทมธ",
        # "#AnthroTU",
    # ],
    # "คณะศิลปศาสตร์":[
    #     "#สินสาดมธ",
    #     "#ศิลปศาสตร์มธ",
    # ],
    # "คณะวารสารศาสตร์และสื่อสารมวลชน":[
    #     "#วารสารมธ",
    # ],
    # "คณะวิทยาศาสตร์และเทคโนโลยี":[
    #     "#วิทยามธ",
    #     "#วิดยามธ",
    # ],
    # "คณะวิศวกรรมศาสตร์":[
    #     "#วิดวะมธ",
    #     "#วิศวะมธ",
    # ],
    # "คณะสถาปัตยกรรมศาสตร์และการผังเมือง":[
    #     "#สถาปัตมธ",
    #     "#ถาปัตมธ",
    #     "#สถาปัตย์มธ",
    # ],
    # "คณะศิลปกรรมศาสตร์":[
    #     "#ละคอนมธ",
    #     "#สินกำมธ",
    #     "#ศิลปกรรมธรรมศาสตร์",
    # ],
    "คณะแพทยศาสตร์":[
        "#แพทย์มธ",
        "#medtu",
    ],
    # "คณะสหเวชศาสตร์":[
    #     "#สหเวชมธ",
    #     "#กายภาพมธ",
    #     "#เทคนิคมธ",
    # ],
    # "คณะทันตแพทยศาสตร์":[
    #     "#ทันตะมธ",
    #     "#DentTU",
    # ],
    # "คณะพยาบาลศาสตร์":[
    #     "#พยาบาลมธ",
    #     "#พยาบาลธรรมศาสตร์",
    # ],
    # "คณะสาธารณสุขศาสตร์":[
    #     "#fphtu",
    #     "#fphthammasat",
    # ],
    # "คณะเภสัชศาสตร์":[
    #     "#เภสัชมธ",
    # ],
    # "คณะวิทยาการเรียนรู้และศึกษาศาสตร์":[
    #     "#lsedtu",
    #     "#lsed",
    #     "#คณะวิทยาการเรียนรู้และศึกษาศาสตร์",
    # ],
    # "วิทยาลัยพัฒนศาสตร์ ป๋วย อึ๊งภากรณ์":[
    #     "#psdsTU",
    #     "#วป๋วย",
    #     "#วิทยาลัยป๋วย",
    #     "#วิทยาลัยพัฒนศาสตร์",
    # ],
    # "วิทยาลัยนวัตกรรม":[
    #     "#นวัตมธ",
    #     "#CITU",
    #     "#CITUSC",
    #     "#CITUTU",
    #     "#InnovationTU",
    #     "#นวัตกรรมธรรมศาสตร์",
    # ],
    "วิทยาลัยสหวิทยาการ":[
        "#CISTU",
        "#สหวิทยาการ",
        "#สหวิทยาการธรรมศาสตร์",
    ],
    # "วิทยาลัยโลกคดีศึกษา":[
    #     "#GSSE",
    #     "#GSSETU",
    # ],
    "สถาบันเทคโนโลยีนานาชาติสิรินธร":[
        "#siittu",
        "#SIIT",
        "#SIITThammasat",
    ],
    # "วิทยาลัยนานาชาติ ปรีดี พนมยงค์":[
    #     "#pbic",
    #     "#pbictu",
    # ],
    # "วิทยาลัยแพทยศาสตร์นานาชาติจุฬาภรณ์":[
    #     "#CICM",
    #     "#CICMTU",
    # ],
    # "สถาบันเสริมศึกษาและทรัพยากรมนุษย์":[
    #     "#tunext",
    # ],
    # "สถาบันไทยคดีศึกษา":[
    #     "#ไทยคดีธรรมศาสตร์",
    #     "#ThaiStudiesTU",
    # ],
    # "สถาบันเอเชียตะวันออกศึกษา":[
    #     "#เอเชียตะวันออกธรรมศาสตร์",
    # ],
    # "สถาบันภาษา":[
    #     "#สถาบันภาษาธรรมศาสตร์",
    # ],
    # "สถาบันอาณาบริเวณศึกษา":[
    #     "#อาณาบริเวณธรรมศาสตร์",
    #     "#AreaStudiesTU",
    # ],
}