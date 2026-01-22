import shapefile
import os

folder = r"C:\Users\guest1\Downloads\CH_D002_00_20260101"
basename = "CH_D002_00_20260101"

shp_path = os.path.join(folder, f"{basename}.shp")
dbf_path = os.path.join(folder, f"{basename}.dbf")

# SHP + DBF 읽기
sf = shapefile.Reader(shp_path, encoding="cp949")

# SHP + DBF를 TXT로 저장
with open(os.path.join(folder, f"{basename}_shp.txt"), "w", encoding="utf-8") as shp_txt:
    for i, shape in enumerate(sf.shapes()):
        shp_txt.write(f"Record {i}:\n")
        shp_txt.write(f"  Shape Type: {shape.shapeType}\n")
        shp_txt.write(f"  Points: {shape.points}\n")
        shp_txt.write("\n")

with open(os.path.join(folder, f"{basename}_dbf.txt"), "w", encoding="utf-8") as dbf_txt:
    fields = [f[0] for f in sf.fields[1:]]  # 첫 필드는 삭제
    dbf_txt.write("\t".join(fields) + "\n")
    for rec in sf.records():
        dbf_txt.write("\t".join(str(val) for val in rec) + "\n")

print("변환 완료!")