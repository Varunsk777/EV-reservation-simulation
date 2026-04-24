# ⚡ EV Charging Reservation Simulation System

A **multi-agent based EV charging station simulation** that models intelligent reservation, priority scheduling, and real-time slot allocation using distributed system concepts.

---

## 🚀 Overview

This project simulates a **coordinator-driven reservation system** for Electric Vehicle (EV) charging stations.
Users (vehicles) send reservation requests, and the system dynamically assigns charging slots based on **priority, availability, and system state**.

---

## 🎯 Key Features

* ⚡ Priority-based scheduling

  * **Priority 1** → Emergency vehicles (Ambulance, Fire Truck)
  * **Priority 2** → VIP / Government vehicles
  * **Priority 3** → Normal vehicles

* 🧠 Multi-Agent Architecture

  * User Agent
  * Coordinator Agent
  * Charging Station Agent

* 🔄 Real-time Simulation

  * Dynamic slot allocation
  * Queue handling
  * Conflict resolution

* ⚙️ Distributed System Concepts

  * Idempotency handling
  * Fault tolerance simulation
  * Event-driven workflow

* 🗄️ Data Management

  * **Redis** → Fast state & caching
  * **PostgreSQL** → Persistent storage

---

## 🏗️ System Architecture

The system consists of multiple interacting components:

* **User Agent** → Sends reservation requests
* **Coordinator** → Core decision-making unit
* **Charging Stations** → Execute reservations
* **Redis** → Temporary fast-access storage
* **PostgreSQL** → Long-term data storage

---

## 🧪 Simulation Workflow

1. User sends a charging request
2. Unique idempotency key is generated
3. Coordinator processes request
4. Priority and availability are evaluated
5. Slot is assigned or queued
6. Reservation status is stored and returned

---

## 🛠️ Tech Stack

* **Language:** Python
* **Backend Logic:** Custom simulation engine
* **Cache / Messaging:** Redis
* **Database:** PostgreSQL
* **Tools:** Git, GitHub

---

## 📂 Project Structure

```bash
.
├── agents/
├── coordinator/
├── simulation/
├── database/
├── redis_layer/
├── main.py
├── requirements.txt
└── README.md
```

---

## ▶️ How to Run

### 1. Clone the repository

```bash
git clone https://github.com/Varunsk777/EV-reservation-simulation.git
cd EV-reservation-simulation
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Redis server

Make sure Redis is running locally.

### 4. Run the simulation

```bash
python main.py
```

---

## 📊 Future Enhancements

* 🌐 Web-based UI dashboard
* 📈 Real-time visualization of station load
* 🤖 AI-based scheduling (RL / DQN integration)
* 📱 Mobile app for booking slots
* ☁️ Cloud deployment (AWS / Docker)

---

## 🤝 Contribution

Contributions are welcome!
Feel free to fork the repo and submit a pull request.

---

## 👨‍💻 Author

**Varun S Kumar**
Aspiring AI & Data Science Engineer

---

## 📜 License

This project is open-source and available under the MIT License.

---
