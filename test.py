from prefect.blocks.system import Secret

print(type(Secret.load('parameters')))