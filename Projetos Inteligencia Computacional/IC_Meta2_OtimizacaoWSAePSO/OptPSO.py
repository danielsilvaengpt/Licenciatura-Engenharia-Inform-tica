### --- 1. IMPORTAÇÃO DE BIBLIOTECAS ---
import numpy as np
import os
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras import layers, models
from tensorflow.keras.optimizers import Adam
from Functions.pso import pso


base_drive_path = r"C:\Users\Daniel\Desktop\3_ano\IC\TP\Dataset\Skin_Diseases\kaggle"
train_otimization_path = os.path.join(base_drive_path, "temp_train")
val_path = os.path.join(base_drive_path, "val")

# --- 2. CONFIGURAÇÃO DOS GERADORES ---
IMG_SIZE_OPT = 64
IMG_SIZE_FINAL = 128
batch_size = 16

print(f"A carregar gerador de treino de otimização (temp_train, {IMG_SIZE_OPT}x{IMG_SIZE_OPT})...")
trainWSA_datagen = ImageDataGenerator(rescale=1. / 255, fill_mode='nearest')
trainWSA_generator = trainWSA_datagen.flow_from_directory(
    train_otimization_path,
    target_size=(IMG_SIZE_OPT, IMG_SIZE_OPT),
    batch_size=batch_size,
    class_mode='categorical',
    shuffle=True
)

print(f"A carregar gerador de validação de otimização (val, {IMG_SIZE_OPT}x{IMG_SIZE_OPT})...")
val_opt_datagen = ImageDataGenerator(rescale=1. / 255)
val_generator_opt = val_opt_datagen.flow_from_directory(
    val_path,
    target_size=(IMG_SIZE_OPT, IMG_SIZE_OPT),
    batch_size=batch_size,
    class_mode='categorical',
    shuffle=False
)

num_classes = trainWSA_generator.num_classes


### --- 3. FUNÇÃO PARA CRIAR O MODELO ---
def create_model(learning_rate, num_neurons, size):
    model = models.Sequential([
        layers.Conv2D(16, (3, 3), activation='relu', input_shape=(size, size, 3)),
        layers.MaxPooling2D((2, 2)),
        layers.Conv2D(32, (3, 3), activation='relu'),
        layers.MaxPooling2D((2, 2)),
        layers.Conv2D(64, (3, 3), activation='relu'),
        layers.Flatten(),
        layers.Dense(int(num_neurons), activation='relu'),
        layers.Dense(num_classes, activation='softmax')
    ])
    optimizer = Adam(learning_rate=learning_rate)
    model.compile(optimizer=optimizer, loss='categorical_crossentropy', metrics=['accuracy'])
    return model


### --- 4. DEFINIÇÃO DA FUNÇÃO DE FITNESS (Melhorada) ---
# Aumentei o limite para 20, mas o EarlyStopping vai parar muito antes se necessário
EPOCHS_FOR_OPTIMIZATION = 10


def fitness_function(params):
    learning_rate = params[0]
    num_neurons = int(params[1])

    # --- ALTERAÇÃO 1: Adicionado EarlyStopping ---

    print(f"Testando: LR={learning_rate:.6f}, Neurónios={num_neurons}...", end=" ")

    model = create_model(learning_rate, num_neurons, IMG_SIZE_OPT)
    history = model.fit(
        trainWSA_generator,
        epochs=EPOCHS_FOR_OPTIMIZATION,
        validation_data=val_generator_opt,
        verbose=0
    )

    val_loss = np.min(history.history['val_loss'])
    epochs_run = len(history.history['val_loss'])  # Para sabermos quantas épocas durou
    print(f"-> Val Loss={val_loss:.5f} ({epochs_run} épocas)")

    return val_loss


### --- 5. EXECUÇÃO DA OTIMIZAÇÃO SWARM (APENAS PSO) ---
n_agentes = 5
n_iteracoes = 10
lb = [0.0001, 32]
ub = [0.01, 128]

print("\n" + "=" * 50)
print("--- INICIANDO OTIMIZAÇÃO DE HIPERPARÂMETROS (PSO) ---")
print(f"Algoritmo usará {n_agentes} agentes e {n_iteracoes} iterações.")
print("=" * 50 + "\n")

print("\n--- Executando Particle Swarm Optimization (PSO) ---")
pso_optimizer = pso(n=n_agentes, function=fitness_function, lb=lb, ub=ub, dimension=2, iteration=n_iteracoes)

print("\nA obter a melhor solução do PSO...")
best_params_pso = pso_optimizer.get_Gbest()

best_fitness_pso = pso_optimizer.get_Gbest_fitness()
print("(Score recuperado da memória - SEM re-treino desnecessário)")


print("--- OTIMIZAÇÃO PSO CONCLUÍDA ---")
print(f"🏆 Melhor Val Loss (PSO): {best_fitness_pso:.5f}")
print(f"🔩 Melhores Hiperparâmetros (PSO):")
print(f"Learning Rate: {best_params_pso[0]:.6f}")
print(f"Neurónios: {int(best_params_pso[1])}")
print("--- Script Concluído ---")