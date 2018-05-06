from __future__ import print_function
from os import path

from schema import *
from rio_database import RioDatabase

class CapitalCost(TechCapitalCost):
    pass


class TechMainCapitalCostLoader(TechMain):
    def __init__(self, name, scenario):
        super(TechMainCapitalCostLoader, self).__init__(name, scenario)
        self.init_from_db(name, scenario)

        # App coder is responsible for loading child data and assigning to instance vars.
        # Every tech has capital cost, so we load this in the superclass
        self.capcost_capacity = CapitalCost.load_from_db(name, scenario, cost_type='capacity')

    def show_costs(self):
        print('\n{} capital cost (capacity)\n{}'.format(self.name, self.capcost_capacity.timeseries()))


class DispatchableThermal(TechMainCapitalCostLoader):
    def __init__(self, name, scenario):
        super(DispatchableThermal, self).__init__(name, scenario)
        self.init_from_db(name, scenario)

        self.startup_cost = TechStartupCost.load_from_db(name, scenario)

        # verify that 'name' refers to a row of the correct type
        assert(self.type == 'DISPATCHABLE THERMAL')

    def show_costs(self):
        super(DispatchableThermal, self).show_costs()
        print('\n{} startup cost {}'.format(self.name, self.startup_cost.value))

class ElectricStorage(TechMainCapitalCostLoader):
    def __init__(self, name, scenario):
        super(ElectricStorage, self).__init__(name, scenario)
        self.init_from_db(name, scenario)

        # verify that 'name' refers to a row of the correct type
        assert(self.type == 'ELECTRIC STORAGE')

        self.capcost_energy = CapitalCost.load_from_db(name, scenario, cost_type='energy')

    def show_costs(self):
        super(ElectricStorage, self).show_costs()
        print('\n{} capital cost (energy)\n{}'.format(self.name, self.capcost_energy.timeseries()))



def main():
    pathname = path.normpath(path.join(path.realpath(__file__), '..', '..', 'test.csvdb'))

    db = RioDatabase.get_database(pathname)
    scenario = None

    # Get the raw data table objects
    tech_main  = db.get_table("TECH_MAIN")
    print("TECH_MAIN:\n", tech_main.data)

    cap_cost_tbl = db.get_table("TECH_CAPITAL_COST")

    # Get copies of dataframe slices
    wind_cap_cost_df = cap_cost_tbl.get_dataframe("WIND")
    print("Wind:\n", wind_cap_cost_df[['vintage', 'value']])

    # Get row as instance from TECH_MAIN
    li_ion_obj = ElectricStorage('LI-ION', scenario)
    li_ion_obj.show_costs()

    # Get a row of raw data as tuple
    ccgt_row = DispatchableThermal.get_row("CCGT", scenario=scenario)
    print("\nCCGT tuple:", ccgt_row)

    ccgt_obj = DispatchableThermal('CCGT', scenario)
    ccgt_obj.show_costs()
    print('done')

if __name__ == '__main__':
    main()
