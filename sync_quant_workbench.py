"""
手动同步量化工作台数据
"""
from pprint import pprint

from quant_workbench.sync import QuantWorkbenchSync


def main():
    syncer = QuantWorkbenchSync()
    result = syncer.run()
    pprint(result)


if __name__ == "__main__":
    main()

