"""
This program will allow users to create,view stock's market and buy/sell shares of a company
"""
import pandas as pd
import requests
import math
from secrets import IEX_CLOUD_API_TOKEN
import boto3
from boto3.dynamodb.conditions import Key, Attr


# This function will check use input.
def check_input(account_name):
    while True:
        if not account_name:
            return False
        else:
            return account_name


# This function will check whether account exist or not.
def exist_account(account_name):
    account_name = account_name.lower()
    dynamodb_client = boto3.client('dynamodb')
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if account_name not in existing_tables:
        print("Account doesn't exist, Create an account.")
        return False
    else:
        return account_name


# This function will create a user S3 bucket and Dynamodb
def create_account():
    while True:
        account_name = input("Enter account name: ")
        if check_input(account_name) == False:
            print("please enter a string.")
        elif exist_account(account_name) == False:
            break
        else:
            print("Account does exist, please try again.")
            return

    # creating Dynamodb table
    dynamodb = boto3.resource('dynamodb')
    primary_key_id = 'Ticker'
    table = dynamodb.create_table(
        TableName=account_name,
        KeySchema=[
            {
                'AttributeName': primary_key_id,
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': primary_key_id,
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }

    )
    print("Table status:", table.table_status)  # Table status
    # download file from S3 bucket
    print()
    print("****** Add Funds ******")
    portfolio_account()
    table = boto3.resource('dynamodb').Table(account_name)
    table.put_item(Item={
        'Ticker': 'Account Balance',
        'Balance': '10'
    })
    s3 = boto3.client('s3')
    s3.download_file('stockbucket1', 'sp_500_stocks.csv', '/home/ec2-user/environment/sp_500_stocks.csv')
    return


# This function will accept user's investment amount.
def portfolio_account():
    account_name = input("Enter account name: ")
    while True:
        if check_input(account_name) == False:
            print("please enter a string.")
        elif exist_account(account_name) == False:
            return
        else:
            print("Account does exist.")
            break

    while True:
        amount = input("Enter amount to invest  (Only numbers greater than $100 only ) :$")
        check_numbers = amount
        if check_numbers.isnumeric() == False:
            print("Number only, Try again : ")
        elif int(float(amount)) <= 100:
            print("Numbers greater than or equal to $100 only, Try again.")
        else:
            amount = check_numbers
            break

    portfolio_new_get = ''
    table = boto3.resource('dynamodb').Table(account_name)
    response = table.query(
        KeyConditionExpression=Key('Ticker').eq('Account Balance')
    )
    for i in response['Items']:
        portfolio_new_get = i['Balance']

    total = math.floor(float(portfolio_new_get) + float(amount))

    table = boto3.resource('dynamodb').Table(account_name)
    table.put_item(Item={
        'Ticker': 'Account Balance',
        'Balance': str(total)
    })

    print('congratulations!!! $' + str(amount) + ' has successful added to your account.')
    return


# lopping a list of 100 items
def chucks(lst, n):
    """Yield successive n-sized chucks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# get the stocks symbols from a save files
def view_stocks():
    stocks = pd.read_csv('sp_500_stocks.csv')
    my_columns = ['Ticker', 'Stock Price', 'Market Capitalization']

    # print(stocks)
    symbol_groups = list(chucks(stocks['Ticker'], 100))
    symbol_strings = []
    for i in range(0, len(symbol_groups)):
        symbol_strings.append(','.join(symbol_groups[i]))
    final_dataframe = pd.DataFrame(columns=my_columns)
    for symbol_string in symbol_strings:
        batch_api_call_url = f'https://sandbox.iexapis.com/stable/stock/market/batch/?types=quote&symbols={symbol_string}&token={IEX_CLOUD_API_TOKEN}'
        data = requests.get(batch_api_call_url).json()
        for symbol in symbol_string.split(','):
            final_dataframe = final_dataframe.append(
                pd.Series(
                    [
                        symbol,
                        data[symbol]['quote']['latestPrice'],
                        data[symbol]['quote']['marketCap']
                    ],
                    index=my_columns
                ),
                ignore_index=True
            )
    print(final_dataframe)


# This function will allow the use to buy stock at the market price.
def buy():
    while True:
        table_name = input("Enter account name: ")
        if check_input(table_name) == False:
            print("Try again, enter a string")
        elif exist_account(table_name) == False:
            print()
        else:
            break

    portfolio_new_get = ''
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.query(
        KeyConditionExpression=Key('Ticker').eq('Account Balance')
    )
    for i in response['Items']:
        portfolio_new_get = i['Balance']
    portfolio = int(portfolio_new_get)

    stocks = pd.read_csv("sp_500_stocks.csv")
    print(stocks)
    stockls_list = []
    for i in stocks['Ticker']:
        stockls_list.append(i)
    while True:
        symbol = input("Enter stock symbol only : ").upper()
        if symbol in stockls_list:
            break
        else:
            print("Stock not found, Please try again.")
    while True:
        share_to_buy = input(f"How many shares of {symbol} do you want to buy? ")
        if share_to_buy.isnumeric() == True:
            break
        else:
            print("Enter Only Numbers")
    api_url = f'https://sandbox.iexapis.com/stable/stock/{symbol}/quote/?token={IEX_CLOUD_API_TOKEN}'
    data = requests.get(api_url).json()
    price = data['latestPrice']
    price_bal = math.floor(float(price) * float(share_to_buy))

    while True:
        if portfolio < price_bal:
            print(f"insufficient funds, try increase your investment account greater than ${str(price_bal)} .")
            return 0
        else:
            portfolio_bal = math.floor(float(portfolio) - float(price_bal))
            my_columns = ['Ticker', 'Stock Price', 'Market Capitalization', 'Number of Shares to Buy', 'Bid Type',
                          'Price', 'Balance']
            final_dataframe = pd.DataFrame(columns=my_columns)
            final_dataframe = final_dataframe.append(pd.Series(
                [
                    symbol,
                    data['latestPrice'],
                    data['marketCap'],
                    share_to_buy,
                    'Buy',
                    price_bal,
                    portfolio_bal
                ],
                index=my_columns
            ),
                ignore_index=True)
            print(final_dataframe)
            table = boto3.resource('dynamodb').Table(table_name)
            table.put_item(Item={
                'Ticker': symbol,
                'Stock Price': str(data['latestPrice']),
                'Market Capitalization': str(data['marketCap']),
                'Number of Shares to Buy': share_to_buy,
                'Bid Type': 'Buy',
                'Total Shares Price': str(price_bal),
                'Balance': str(portfolio_bal)
            })
            table.put_item(Item={'Ticker': 'Account Balance', 'Balance': str(portfolio_bal)})
        break


# This function will allow the user to sell a stock at the market price.
def sell():
    share_to_sell = ''
    item = []
    while True:
        table_name = input("Enter account name: ")
        if check_input(table_name) == False:
            print("Try again.")
        elif exist_account(table_name) == False:
            print("Try again.")
        else:
            break
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.scan(FilterExpression=Attr('Bid Type').eq('Buy'))
    items = response['Items']
    print("\n****** Available Stocks to sell ******\n")
    for i in items:
        print(i['Ticker'], i['Bid Type'])
        item.append(i['Ticker'])
    while True:
        symbol = input("Enter stock symbol only : ").upper()
        if not symbol:
            print("please try again, Enter stock symbol only")
        elif symbol not in item:
            print("Please select stock form your account. Try again.")
        else:
            break
    response = table.query(
        KeyConditionExpression=Key('Ticker').eq(symbol)
    )
    for i in response['Items']:
        share_to_sell = i['Number of Shares to Buy']

    api_url = f'https://sandbox.iexapis.com/stable/stock/{symbol}/quote/?token={IEX_CLOUD_API_TOKEN}'
    data = requests.get(api_url).json()
    price = data['latestPrice']
    price_bal = float(price) * float(share_to_sell)
    portfolio_new_get = ''
    table = boto3.resource('dynamodb').Table(table_name)
    response = table.query(
        KeyConditionExpression=Key('Ticker').eq('Account Balance')
    )
    for i in response['Items']:
        portfolio_new_get = i['Balance']
    portfolio = float(portfolio_new_get)
    portfolio_bal = math.floor(float(portfolio) + float(price_bal))
    my_columns = ['Ticker', 'Stock Price', 'Market Capitalization', 'Number of Shares', 'Bid Type', 'Price', 'Balance']
    final_dataframe = pd.DataFrame(columns=my_columns)
    final_dataframe = final_dataframe.append(pd.Series(
        [
            symbol,
            data['latestPrice'],
            data['marketCap'],
            share_to_sell,
            'Sell',
            price_bal,
            portfolio_bal
        ],
        index=my_columns
    ),
        ignore_index=True)
    print(final_dataframe)
    table = boto3.resource('dynamodb').Table(table_name)
    table.put_item(Item={
        'Ticker': symbol,
        'Stock Price': str(data['latestPrice']),
        'Market Capitalization': str(data['marketCap']),
        'Number of Shares to sell': share_to_sell,
        'Bid Type': 'Sell',
        'Total Shares Price': str(price_bal),
        'Balance': str(portfolio_bal)
    })
    table.put_item(Item={'Ticker': 'Account Balance', 'Balance': str(portfolio_bal)})
    return


def main():
    print(" Welcome to S&P 500 stocks.\n", "_" * 25)
    menus = {"0: Exit",
             "1: View S&P 500",
             "2: Create an account",
             "3: buy shares",
             "4: Sell shares",
             "5: Add funds"
             }
    for menu in sorted(menus):
        print(menu)

    while True:
        user_input = input("\nMain Menu select: ")
        if user_input == "0":
            return print("Thanks for using Franklin's Stock system")
        elif user_input == "1":
            view_stocks()
        elif user_input == "2":
            create_account()
        elif user_input == "3":
            buy()
        elif user_input == "4":
            sell()
        elif user_input == "5":
            portfolio_account()
        else:
            print("Invalid input,try again.")

main()
