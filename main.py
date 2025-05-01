from prefect import flow

@flow
def hello():
    print("Hello world")

hello()
    #test 