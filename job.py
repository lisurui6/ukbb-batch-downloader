# Copyright 2017, Wenjia Bai. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""
    The script downloads the cardiac MR images for a UK Biobank Application and
    converts the DICOM into nifti images.
    """

import os
import glob
import pandas as pd
# from biobank_utils import process_manifest, Biobank_Dataset
import dateutil.parser
from pathlib import Path
from tqdm import tqdm
import multiprocessing as mp
import shutil
from argparse import ArgumentParser


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("--csv-file", dest="csv_file", type=str, required=True, help="List of EIDs to download, column name eid")
    parser.add_argument("--key-path", dest="key_path", type=str, required=True)
    parser.add_argument("--ukbfetch-path", dest="ukbfetch_path", type=str, required=True)
    parser.add_argument("--output-dir", dest="output_dir", type=str, required=True)
    parser.add_argument("--n-thread", dest="n_thread", type=int, default=0)

    return parser.parse_args()


def function(eid, ukbkey: Path, ukbfetch: Path, output_dir: Path):
    output_image_dir = output_dir.joinpath("images")

    zip_dir = output_image_dir.joinpath("zip")
    zip_dir.mkdir(parents=True, exist_ok=True)

    # Create a batch file for this subject
    batch_file = output_dir.joinpath("batch", f"{eid}_batch")
    batch_file.parent.mkdir(parents=True, exist_ok=True)
    # batch_file = os.path.join(data_dir, '{0}_batch'.format(eid))
    with open(str(batch_file), 'w') as f_batch:
        for j in range(20208, 20210):
            # The field ID information can be searched at http://biobank.ctsu.ox.ac.uk/crystal/search.cgi
            # 20208: Long axis heart images - DICOM Heart MRI
            # 20209: Short axis heart images - DICOM Heart MRI
            # 2.0 means the 2nd visit of the subject, the 0th data item for that visit.
            # As far as I know, the imaging scan for each subject is performed at his/her 2nd visit.
            field = '{0}-2.0'.format(j)
            f_batch.write('{0} {1}_2_0\n'.format(eid, j))

    # Download the data using the batch file
    # ukbfetch = os.path.join(util_dir, 'ukbfetch')
    # print('Downloading data for subject {} ...'.format(eid))
    command = '{0} -b{1} -a{2}'.format(ukbfetch, str(batch_file), ukbkey)
    os.system('{0} -b{1} -a{2}'.format(ukbfetch, str(batch_file), ukbkey))
    print("Download finished")
    # Unpack the data
    for f in Path(__file__).parent.glob('{0}_*.zip'.format(eid)):
        shutil.move(str(f), str(zip_dir.joinpath(f.name)))
    print("Move finished")

    batch_file.unlink()
    # files = glob.glob('{0}_*.zip'.format(eid))
    # for f in files:
    #     os.system('unzip -o {0} -d {1}'.format(f, str(dicom_dir)))
    #
    #     # Process the manifest file
    #     if dicom_dir.joinpath('manifest.cvs').exists():
    #         os.system('cp {0} {1}'.format(str(dicom_dir.joinpath('manifest.cvs')),
    #                                       str(dicom_dir.joinpath('manifest.csv'))))
    #     process_manifest(str(dicom_dir.joinpath('manifest.csv')),
    #                      str(dicom_dir.joinpath('manifest2.csv')))
    #     df2 = pd.read_csv(str(str(dicom_dir.joinpath('manifest2.csv'))), error_bad_lines=False)
    #
    #     # Patient ID and acquisition date
    #     pid = df2.at[0, 'patientid']
    #     date = dateutil.parser.parse(df2.at[0, 'date'][:11]).date().isoformat()
    #
    #     # Organise the dicom files
    #     # Group the files into subdirectories for each imaging series
    #     for series_name, series_df in df2.groupby('series discription'):
    #         # series_dir = os.path.join(dicom_dir, series_name)
    #         series_dir = dicom_dir.joinpath(series_name)
    #         series_dir.mkdir(parents=True, exist_ok=True)
    #         # if not os.path.exists(series_dir):
    #         #     os.mkdir(series_dir)
    #         series_files = [os.path.join(str(dicom_dir), x) for x in series_df['filename']]
    #         os.system('mv {0} {1}'.format(' '.join(series_files), series_dir))
    #
    # # Convert dicom files and annotations into nifti images
    # # dset = Biobank_Dataset(str(dicom_dir))
    # # dset.read_dicom_images()
    # # dset.convert_dicom_to_nifti(str(data_dir))
    #
    # # Remove intermediate files
    # # os.system('rm -rf {0}'.format(str(dicom_dir)))
    # os.system('rm -f {0}'.format(str(batch_file)))
    # os.system('rm -f {0}_*.zip'.format(eid))

    return "finished"


def main():
    args = parse_args()
    # Where the data will be downloaded
    # main_dir = Path("Z:\\UKBB_40616\\new_download")
    csv_file = Path(args.csv_file)
    key_path = Path(args.key_path)
    ukbfetch_path = Path(args.ukbfetch_path)
    output_dir = Path(args.output_dir)
    n_thread = args.n_thread
    df = pd.read_csv(str(csv_file))
    data_list = df['eid']

    # Download cardiac MR images for each subject
    start_idx = 0
    end_idx = len(data_list)
    pbar = tqdm(range(start_idx, end_idx))

    if n_thread == 0:
        for i in pbar:
            eid = str(data_list[i])
            function(eid, key_path, ukbfetch_path, output_dir)
    else:
        def update(*a):
            pbar.update()
        pool = mp.Pool(processes=n_thread)
        # setup multiprocessing
        for i in range(pbar.total):
            eid = str(data_list[i])
            pool.apply_async(func=function, args=(eid, key_path, ukbfetch_path, output_dir), callback=update)
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()
