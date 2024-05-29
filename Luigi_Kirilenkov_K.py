import luigi
import wget
import logging
import gzip
import tarfile
import os
import io
import shutil
import pandas as pd


# Название сета данных
DATASET = 'GSE68849'
# Url для загрузки сета
URL = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={DATASET}&format=file'


class ExtractData(luigi.Task):
    dataset_name = luigi.Parameter(default=DATASET)
    url = luigi.Parameter(default=URL)

    def output(self):
        return luigi.LocalTarget(os.path.join('extracted', self.dataset_name))

    def run(self):
        # Убедимся, что директория для данных существует
        if not os.path.exists('data'):
            os.makedirs('data')

        # Загрузка датасета
        tar_path = os.path.join('data', self.dataset_name + '_RAW.tar')
        logging.info(f"Загрузка {self.dataset_name}...")
        wget.download(self.url, tar_path)

        # Распаковка датасета
        extract_path = self.output().path
        if not os.path.exists(extract_path):
            os.makedirs(extract_path)

        with tarfile.open(tar_path) as tar:
            logging.info(f"Распаковка {self.dataset_name}...")
            tar.extractall(path=extract_path)

        os.remove(tar_path)  # Удаление архива после распаковки

        # Обработка каждого файла внутри архива
        for item in os.listdir(extract_path):
            if item.endswith('.gz'):
                self.process_gzip_file(os.path.join(extract_path, item), extract_path)

    def process_gzip_file(self, gz_path, extract_path):
        filename = os.path.splitext(os.path.basename(gz_path))[0]
        output_path = os.path.join(extract_path, filename)
        with gzip.open(gz_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                logging.info(f"Распаковка {gz_path}")
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_path)  # Удаление gz файла после распаковки


class TransformData(luigi.Task):
    dataset_name = luigi.Parameter(default=DATASET)
    url = luigi.Parameter(default=URL)

    def requires(self):
        return ExtractData(dataset_name=self.dataset_name, url=self.url)

    def run(self):
        extract_path = self.input().path
        for filename in os.listdir(extract_path):
            file_path = os.path.join(extract_path, filename)
            if not filename.endswith('.txt'):
                continue
            self.process_text_file(file_path, filename)

    def process_text_file(self, file_path, filename):
        dfs = {}
        with open(file_path) as file:
            fio = io.StringIO()
            write_key = None
            for line in file:
                if line.startswith('['):
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header='infer')
                        fio = io.StringIO()
                    write_key = line.strip('[]\n')
                    continue
                fio.write(line)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Создание директории для сохранения файлов
        parent_dir = os.path.join(self.output().path, os.path.splitext(filename)[0])
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir)

        for key, df in dfs.items():
            output_path = os.path.join(parent_dir, f'{os.path.splitext(filename)[0]}_{key}.tsv')
            logging.info(f"Сохранение таблицы {key} в {output_path}")
            df.to_csv(output_path, sep='\t', index=False)

            if key == "Probes":
                probes_reduced_df = df.drop(
                    columns=["Definition", "Ontology_Component", "Ontology_Process", "Ontology_Function", "Synonyms", "Obsolete_Probe_Id", "Probe_Sequence"]
                )
                reduced_output_path = os.path.join(parent_dir, f'{os.path.splitext(filename)[0]}_Probes_reduced.tsv')
                logging.info(f"Сохранение сокращенной таблицы Probes в {reduced_output_path}")
                probes_reduced_df.to_csv(reduced_output_path, sep='\t', index=False)

    def output(self):
        return luigi.LocalTarget(os.path.join('processed', self.dataset_name))


class LoadData(luigi.Task):
    dataset_name = luigi.Parameter(default=DATASET)
    url = luigi.Parameter(default=URL)

    def requires(self):
        return TransformData(dataset_name=self.dataset_name, url=self.url)

    def run(self):
        # Эта задача будет использоваться для загрузки данных в некоторое место назначения (например, базу данных).
        # Для этого примера мы просто логируем завершение.
        logging.info(f"Загрузка данных для {self.dataset_name} завершена.")

    def output(self):
        return luigi.LocalTarget(os.path.join('loaded', self.dataset_name))


if __name__ == '__main__':
    luigi.build([LoadData(DATASET, URL)], local_scheduler=True)
