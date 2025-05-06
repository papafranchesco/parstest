from openai import OpenAI
client = OpenAI(api_key='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')

for m in client.models.list().data:   # вернёт только то, к чему у вашего ключа есть доступ
    print(m.id)
