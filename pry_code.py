# Mejorar esto!!!
CODE = """
from pyspark.sql import DataFrame

import utils


class DataSourcesGenerationMetaClass(type):
    @property
    def spark(cls):
        if getattr(cls, "_spark", None) is None:
            cls._spark = get_spark_session()
        return cls._spark

        
class DataSourcesGeneration(metaclass=DataSourcesGenerationMetaClass):
    QUERY_JOIN: str = None

    @classmethod
    def get_dimentions(cls, datasource: utils.Datasource) -> Dict[str, DataFrame]:
        dimentions = {}
        dimentions["prod"] = utils.get_products(datasource)
        dimentions["pdv"] = utils.get_pdvs(datasource)
        return dimentions
    
    @staticmethod
    def get_hechos(datasource: utils.Datasource) -> DataFrame:
        return utils.get_hechos(datasource)
    
    @classmethod
    def run(cls, datasource: utils.Datasource) -> DataFrame:
        dimentions = cls.get_dimentions(datasource)
        cls.get_hechos(datasource).createOrReplaceTempView("v")

        for key, df in dimentions:
            df.createOrReplaceTempView(key)
        
            if not cls.QUERY_JOIN:
                cls.QUERY_JOIN = utils.get_query_join(datasource)
            
                return cls.spark.sql(cls.QUERY_JOIN)
    
    @classmethod
    def main(cls, datasource_id: int = None, p: str = None, datasource: utils.Datasource = None):
        if not datasource:
            datasource = utils.get_datasource_by_id(p=p, id=datasource_id)
        
        df = cls.run(datasource)

        path = utils.get_dir_persist_datasource(datasource.name)
        path_ds = f"{path}/{datasource.name}/"

        df.coalesce(1).write.csv(path_ds, header=True, sep=";", mode="overwrite")


"""
