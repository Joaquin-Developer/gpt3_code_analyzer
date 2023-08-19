import os
import openai

from pry_code import CODE

openai.api_key = os.environ.get("OPENAI_KEY") or "KEY"


def main():
    question = "¿Qué mejoras puedo hacer en mi proyecto?"

    request = f"Fragmento de código del proyecto:\n{CODE}\nPregunta: {question}"

    response = openai.Completion.create(
        engine="text-davinci-003",  # Elige el motor adecuado
        prompt=request,
        max_tokens=100  # Ajusta según la longitud deseada de la respuesta
    )

    print(response.choices[0].text.strip())


if __name__ == "__main__":
    main()
