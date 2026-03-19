"""合成データ生成スクリプト.

現実的な相関構造を持つコンテスト用合成データを生成する。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

# 属性定義
OCCUPATIONS = [
    "engineer", "teacher", "doctor", "nurse", "lawyer",
    "accountant", "designer", "sales", "manager", "researcher",
    "clerk", "driver", "chef", "farmer", "writer",
    "student", "retired", "self_employed", "civil_servant", "other",
]

EDUCATIONS = ["high_school", "vocational", "bachelor", "master", "doctor_degree"]

DISEASES = [
    "diabetes", "flu", "cancer", "heart_disease", "asthma",
    "allergy", "depression", "hypertension", "obesity", "arthritis",
    "migraine", "pneumonia", "bronchitis", "anemia", "healthy",
]

HOBBIES = [
    "reading", "sports", "music", "cooking", "travel",
    "gaming", "photography", "gardening", "fishing", "art",
    "movies", "hiking", "yoga", "dancing", "crafts",
    "volunteering", "programming", "shopping", "pets", "none",
]

# 職業→学歴の条件付き確率（高学歴ほど専門職に多い）
EDUCATION_WEIGHTS_BY_OCCUPATION = {
    "doctor": [0.0, 0.0, 0.1, 0.2, 0.7],
    "lawyer": [0.0, 0.0, 0.2, 0.5, 0.3],
    "researcher": [0.0, 0.0, 0.1, 0.4, 0.5],
    "engineer": [0.05, 0.1, 0.4, 0.35, 0.1],
    "teacher": [0.05, 0.1, 0.5, 0.3, 0.05],
    "manager": [0.1, 0.1, 0.5, 0.25, 0.05],
    "student": [0.3, 0.2, 0.4, 0.1, 0.0],
}

# 郵便番号の生成（地域をシミュレート）
ZIPCODE_PREFIXES = [
    "100", "101", "102", "103", "104", "105",
    "150", "151", "152", "153", "154", "155",
    "160", "161", "162", "163", "164", "165",
    "200", "201", "210", "211", "220", "221",
    "300", "310", "330", "331", "400", "410",
    "500", "510", "530", "540", "600", "601",
    "700", "710", "730", "770", "800", "810",
    "900", "901",
]


def generate_dataset(n_records: int, seed: int = 42) -> pd.DataFrame:
    """合成データを生成する.

    Args:
        n_records: レコード数
        seed: 乱数シード

    Returns:
        生成されたDataFrame
    """
    rng = np.random.RandomState(seed)

    # ID
    ids = np.arange(1, n_records + 1)

    # 年齢: 正規分布ベース（18〜90）
    ages = np.clip(rng.normal(loc=45, scale=15, size=n_records).astype(int), 18, 90)

    # 性別
    genders = rng.choice(["M", "F", "Other"], size=n_records, p=[0.48, 0.48, 0.04])

    # 職業: 年齢に依存
    occupations = []
    for age in ages:
        if age < 23:
            occ = rng.choice(OCCUPATIONS, p=_student_occupation_probs())
        elif age >= 65:
            occ = rng.choice(OCCUPATIONS, p=_retired_occupation_probs())
        else:
            occ = rng.choice(OCCUPATIONS)
        occupations.append(occ)
    occupations = np.array(occupations)

    # 学歴: 職業に依存
    educations = []
    for occ in occupations:
        weights = EDUCATION_WEIGHTS_BY_OCCUPATION.get(occ, [0.2, 0.15, 0.4, 0.2, 0.05])
        educations.append(rng.choice(EDUCATIONS, p=weights))
    educations = np.array(educations)

    # 郵便番号
    zipcodes = []
    for _ in range(n_records):
        prefix = rng.choice(ZIPCODE_PREFIXES)
        suffix = f"{rng.randint(0, 10000):04d}"
        zipcodes.append(f"{prefix}-{suffix}")
    zipcodes = np.array(zipcodes)

    # 年収: 年齢・学歴・職業に依存
    salaries = []
    for age, edu, occ in zip(ages, educations, occupations):
        base = _base_salary(age, edu, occ)
        noise = rng.normal(0, base * 0.15)
        sal = int(np.clip(base + noise, 2000000, 20000000))
        # 10万円単位に丸め
        sal = (sal // 100000) * 100000
        salaries.append(sal)
    salaries = np.array(salaries)

    # 疾病: 年齢に依存
    diseases = []
    for age in ages:
        diseases.append(rng.choice(DISEASES, p=_disease_probs(age)))
    diseases = np.array(diseases)

    # 趣味
    hobbies = rng.choice(HOBBIES, size=n_records)

    df = pd.DataFrame(
        {
            "id": ids,
            "age": ages,
            "gender": genders,
            "zipcode": zipcodes,
            "occupation": occupations,
            "education": educations,
            "disease": diseases,
            "salary": salaries,
            "hobby": hobbies,
        }
    )
    return df


def _student_occupation_probs() -> list[float]:
    """学生が多い職業分布."""
    probs = [0.01] * len(OCCUPATIONS)
    idx = OCCUPATIONS.index("student")
    probs[idx] = 0.7
    # 残りを正規化
    remaining = (1.0 - 0.7) / (len(OCCUPATIONS) - 1)
    for i in range(len(probs)):
        if i != idx:
            probs[i] = remaining
    return probs


def _retired_occupation_probs() -> list[float]:
    """高齢者が多い職業分布."""
    probs = [0.01] * len(OCCUPATIONS)
    idx = OCCUPATIONS.index("retired")
    probs[idx] = 0.5
    remaining = (1.0 - 0.5) / (len(OCCUPATIONS) - 1)
    for i in range(len(probs)):
        if i != idx:
            probs[i] = remaining
    return probs


def _base_salary(age: int, education: str, occupation: str) -> float:
    """ベース年収を算出."""
    # 学歴係数
    edu_factor = {
        "high_school": 0.7,
        "vocational": 0.8,
        "bachelor": 1.0,
        "master": 1.2,
        "doctor_degree": 1.4,
    }.get(education, 1.0)

    # 職業係数
    occ_factor = {
        "doctor": 1.6,
        "lawyer": 1.5,
        "manager": 1.3,
        "engineer": 1.2,
        "researcher": 1.15,
        "accountant": 1.1,
        "teacher": 1.0,
        "civil_servant": 1.0,
        "self_employed": 1.1,
        "student": 0.4,
        "retired": 0.6,
    }.get(occupation, 0.9)

    # 年齢による昇給カーブ（ピークは50歳前後）
    if age < 25:
        age_factor = 0.6
    elif age < 35:
        age_factor = 0.8 + (age - 25) * 0.02
    elif age < 50:
        age_factor = 1.0 + (age - 35) * 0.01
    elif age < 60:
        age_factor = 1.15
    else:
        age_factor = 0.8

    return 4500000 * edu_factor * occ_factor * age_factor


def _disease_probs(age: int) -> list[float]:
    """年齢に依存する疾病確率."""
    probs = [1.0 / len(DISEASES)] * len(DISEASES)

    # healthy のインデックス
    healthy_idx = DISEASES.index("healthy")

    if age < 30:
        probs[healthy_idx] = 0.4
    elif age < 50:
        probs[healthy_idx] = 0.25
    else:
        probs[healthy_idx] = 0.1
        # 高齢者は生活習慣病が増加
        for disease_name in ["diabetes", "heart_disease", "hypertension", "arthritis"]:
            idx = DISEASES.index(disease_name)
            probs[idx] = 0.12

    # 正規化
    total = sum(probs)
    return [p / total for p in probs]


def main() -> None:
    parser = argparse.ArgumentParser(description="合成データ生成")
    parser.add_argument("--output-dir", type=str, default="data/original")
    parser.add_argument("--sample-size", type=int, default=1000)
    parser.add_argument("--qualifying-size", type=int, default=30000)
    parser.add_argument("--final-size", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    datasets = [
        ("sample.csv", args.sample_size, args.seed),
        ("qualifying.csv", args.qualifying_size, args.seed + 1),
        ("final.csv", args.final_size, args.seed + 2),
    ]

    for filename, size, seed in datasets:
        print(f"生成中: {filename} ({size}件, seed={seed})")
        df = generate_dataset(size, seed)
        path = output_dir / filename
        df.to_csv(path, index=False)
        print(f"  保存: {path}")
        print(f"  年齢: mean={df['age'].mean():.1f}, std={df['age'].std():.1f}")
        print(f"  年収: mean={df['salary'].mean():.0f}, std={df['salary'].std():.0f}")
        print()


if __name__ == "__main__":
    main()
