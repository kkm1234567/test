import os
import glob
import geopandas as gpd

def shp_to_csv_all(folder_path):
	shp_files = glob.glob(os.path.join(folder_path, '*.shp'))
	if not shp_files:
		print('No .shp files found in', folder_path)
		return
	for shp_file in shp_files:
		print('Processing:', shp_file)
		gdf = gpd.read_file(shp_file)
		csv_file = shp_file.replace('.shp', '.csv')
		gdf.to_csv(csv_file, index=False, encoding='utf-8')
		print('Saved:', csv_file)

if __name__ == "__main__":
	folder = r"C:\Users\guest1\Downloads\서울 도시계획사업(서울플랜+) 공간정보_2509\서울 도시계획사업(서울플랜+) 공간정보"
	shp_to_csv_all(folder)
