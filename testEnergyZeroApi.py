import asyncio

from energyzero import EnergyZero, VatOption
from datetime import datetime, date, timedelta

import pytz

# en deze?
# https://mijn.easyenergy.com/nl/api/tariff/getapxtariffs?startTimestamp=2020-04-29T22%3A00%3A00.000Z&endTimestamp=2020-04-30T22%3A00%3A00.000Z&grouping=
# https://pypi.org/project/easyenergy/
#
# start = datetime.now() - timedelta(days=1)
# end = datetime.now() + timedelta(days=1)

# async def main() -> None:
#     """Show example on fetching the energy prices from EnergyZero."""
#     async with EnergyZero(vat=VatOption.INCLUDE) as client:
#         start_date = start
#         end_date = end # date(2022, 12, 7)
#
#         energy = await client.energy_prices(start_date, end_date)
#         gas = await client.gas_prices(start_date, end_date)
#         print(energy)

async def main() -> None:
    """Show example on fetching the energy prices from EnergyZero."""
    async with EnergyZero(vat=VatOption.INCLUDE) as client:
        local = pytz.timezone("CET")
        yesterday = date.today() - timedelta(days=1)
        today = date.today()
        tomorrow = date.today() + timedelta(days=1)

        energy_today = await client.energy_prices(start_date=today, end_date=today)

        print(energy_today)


        print("--- ENERGY TODAY ---")
        print(f"Max price: €{energy_today.extreme_prices[1]}")
        print(f"Min price: €{energy_today.extreme_prices[0]}")
        print(f"Average price: €{energy_today.average_price}")
        print(f"Percentage: {energy_today.pct_of_max_price}%")
        print()
        print(
            f"High time: {energy_today.highest_price_time.astimezone(local)}",
        )
        print(
            f"Lowest time: {energy_today.lowest_price_time.astimezone(local)}",
        )
        print()
        print(f"Current hourprice: €{energy_today.current_price}")
        next_hour = energy_today.utcnow() + timedelta(hours=1)
        print(f"Next hourprice: €{energy_today.price_at_time(next_hour)}")
        best_hours = energy_today.hours_priced_equal_or_lower
        print(f"Hours lower or equal than current price: {best_hours}")

        print()
        print("--- ENERGY TOMORROW ---")
        try:
            energy_tomorrow = await client.energy_prices(
                start_date=tomorrow,
                end_date=tomorrow,
            )


            print(f"Max price: €{energy_tomorrow.extreme_prices[1]}")
            print(f"Min price: €{energy_tomorrow.extreme_prices[0]}")
            print(f"Average price: €{energy_tomorrow.average_price}")
            print()
            time_high = energy_tomorrow.highest_price_time.astimezone(local)
            print(f"Highest price time: {time_high}")
            time_low = energy_tomorrow.lowest_price_time.astimezone(local)
            print(f"Lowest price time: {time_low}")
        except Exception as e:
            print(e)

if __name__ == "__main__":
    asyncio.run(main())