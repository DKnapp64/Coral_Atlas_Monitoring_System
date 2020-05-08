import os

import gdal
import numpy as np


def generate_baseline(local_input_dir: str, output_file_path: str) -> None:
    '''Generate a single-band baseline of the max of the input quads from each pixel

    Args:
      local_input_dir - Full path the local directory containing the input quads
      output_file_path - Full path, including file name, of the output baseline TIF
    '''

    full_input_file_paths = [os.path.join(local_input_dir, f) for f in os.listdir(local_input_dir)]
    baseline_datasets = [gdal.Open(f, gdal.GA_ReadOnly) for f in full_input_file_paths]
    baseline_arrays = [ds.GetRasterBand(1).ReadAsArray() for ds in baseline_datasets]
    baseline_array_3d = np.stack(baseline_arrays, axis=2)
    baseline_array_2d = np.nanmax(baseline_array_3d, axis=2)

    driver = gdal.GetDriverByName('GTiff')
    driver.Register()
    
    outDataset = driver.Create(output_file_path,
                               xsize=baseline_datasets[0].RasterXSize,
                               ysize=baseline_datasets[0].RasterYSize,
                               bands=1,
                               eType=gdal.GDT_Int16,
                               options=['BIGTIFF=YES', 'COMPRESS=LZW', 'TILED=YES'])
    outDataset.SetProjection(baseline_datasets[0].GetProjection())
    outDataset.SetGeoTransform(baseline_datasets[0].GetGeoTransform())

    outDataset.GetRasterBand(1).WriteArray(baseline_array_2d)
    outDataset.GetRasterBand(1).SetNoDataValue(-9999)

    del outDataset
