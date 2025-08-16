# Meta TaskWave: Meta-Selection for Hybrid Dynamic Scheduling in FaaS Environments

**Author:** Theresa To<br>
**Supervisor:** Dr. Chris Phillips  
**Institution:** Queen Mary Unvieristy of London

## Project Overview

Meta TaskWave proposes a transparent, benchmark-driven framework for evaluating hybrid scheduling strategies in Function-as-a-Service (FaaS) platforms. Rather than creating a new optimiser, the project focuses on **meta-selection** — the systematic exposure and classification of scheduling trade-offs across job and worker algorithms.

The project implements **15 hybrid scheduling strategies** (3 job-side x 5 worker-side algorithms) evaluated across three workload intensities using a reproducible benchmarking protocol. The goal is to expose the performance regimes of different strategies and inform adaptive scheduling through structured insights.

## Research Structure

This project follows a three-part research methodology:

### Part 1: System Design
Implementation of a distributed FaaS simulation system with realistic constraints and configurable scheduling algorithms.

### Part 2: Benchmarking Methodology
Development of a rigorous experimental protocol for comparative evaluation of hybrid scheduling strategies.

### Part 3: Statistical Evaluation
Comprehensive analysis framework employing multiple statistical techniques to validate findings and establish algorithmic trade-offs.

## System Architecture
The Meta TaskWave system comprises three core components that simulate a realistic FaaS environment:

### Job Generator (`job-generator.py`)
**Purpose:** Synthetic workload creation with configurable patterns
- **Batch Control:** 4-8 jobs per batch reflecting production FaaS patterns (Shahrad et al., 2020; Joosen et al., 2023)
- **Priority Distribution:** 30% high-priority jobs based on Azure Functions analysis (Shahrad et al., 2020)
- **Inter-arrival Timing:** Bursty batch arrivals validated against Huawei cloud traces (Joosen et al., 2023)
- **Load Variation:** 200-600MB job sizes following documented serverless application patterns (Eismann et al., 2022)
- **Workload Characterisation:** Based on SeBS benchmark methodology (Copik et al., 2021)

### Central Scheduler (`scheduler.py`)
**Purpose:** Hybrid algorithm implementation and intelligent job assignment

**Algorithm Foundation:**
- **Kubernetes Scheduling:** Load balancing and fairness principles (Kaur et al., 2020; Chu et al., 2025)
- **GPU Orchestration:** Dynamic container orchestration strategies (Thinakaran et al., 2019)
- **Heterogeneous Resource Management:** Multi-objective scheduling approaches (Vandebon et al., 2021)

#### Algorithm Combinations:

**Job-Side Algorithms (3):**
- **Round Robin (RR):** FIFO baseline preventing job starvation 
- **Earliest Deadline First (EDF):** Real-time scheduling theory for time-sensitive workload
- **Urgency First (UF):** Priority-deadline balancing inspired by multi-objective cloud scheduling

**Worker-Side Algorithms (5):**
- **Random:** Statistical baseline for comparative analysis
- **Round Robin:** Fair distribution based on container orchestration patterns
- **Least Loaded Fair:** Load-balancing with fairness constraints from Kubernetes researc
- **Fastest Worker Fair:** Throughput maximisation following GPU orchestration principles
- **Network Optimal Fair:** Communication overhead minimisation based on bandwidth-sharing strategies

**Total Combinations:** 15 hybrid strategies (3 x 5 matrix)

### Worker Pool (`worker.py`)
**Purpose:** Heterogeneous workers with realistic FaaS constraints validated by production studies

- **Capacity Variation:** 600-1200MB memory allocation following AWS Lambda documentation (AWS, 2025)
- **Cold Start Penalties:** 300ms base penalty based on multiple provider studies (Ustiugov et al., 2021; Silva et al., 2020)
- **Container Lifecycle:** HOT/WARM/COLD transitions following serverless caching research (Fuerst & Sharma, 2021)
- **Performance Modeling:** Processing variations based on serverless performance evaluation (Scheuner & Leitner, 2020; Mahmoudi & Khazaei, 2022)
- **Workload Analysis:** DAG transformation and characterisation patterns (Mahgoub et al., 2022)

### Container Lifecycle Model
Meta TaskWave implements a three-tier thermal state model that simulates container lifecycle behaviour in FaaS environments:

- **HOT State:** ≤10s idle (no penalty) - Container is warm and ready for immediate execution
- **WARM State:** >10s and ≤30s idle (20% penalty) - Container requires partial reactivation
- **COLD State:** >30s idle (full 300ms penalty) - Container requires complete cold start

These states use a 1:60 time compression factor: 1 second in simulation represents 60 seconds in real FaaS environments. This scaling maintains realistic experimental duration whilst preserving authentic thermal dynamics observed in production systems.

The step-function model reflects real-world container management where resources are reclaimed in stages rather than gradually, directly impacting scheduling performance through cold start penalties.

## Benchmarking Methodology

### Literature-Validated Experimental Design
The benchmarking protocol follows established practices from serverless performance research:

**Benchmark Structure:** Three-phase approach based on SeBS methodology (Copik et al., 2021) and performance stability research (Eismann et al., 2022)

**Workload Intensities:** Based on production trace analysis from multiple cloud providers:
- **Light Workload:** 8-15s inter-arrival delays (Joosen et al., 2023 - low-activity patterns)
- **Moderate Workload:** 2-5s inter-arrival delays (Shahrad et al., 2020 - typical production)
- **Heavy Workload:** 0.5-1.5s inter-arrival delays (Joosen et al., 2023 - peak traffic bursts)

**Performance Metrics:** Comprehensive coverage following FaaS performance evaluation frameworks (Scheuner & Leitner, 2020; Mahmoudi & Khazaei, 2022)

## Statistical Evaluation Framework
Multi-layered statistical approach validated against established practices in distributed systems research:

### Analysis Components
1. **Distribution Analysis:** Coefficient of Variation following cloud benchmarking standards (Folkerts et al., 2013)
2. **Variance Component Analysis:** ANOVA methodology from cloud scheduling research (Wang, 2024; Chu et al., 2025)
3. **Categorical Association Analysis:** Chi-square and Cramér's V from multi-objective scheduling analysis (Lin et al., 2019)
4. **Thermal Efficiency Analysis:** Novel container lifecycle metrics building on cold start research (Fuerst & Sharma, 2021; Romero et al., 2021)
5. **Cross-Validation:** Correlation validation ensuring methodological robustness


## Installation and Setup

### Prerequisites
- Python 3.8+
- Required packages listed in `requirements.txt`

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Verify installation
python config.py
```

## Usage Instructions

### Complete Benchmark Suite

**Recommended primary research command:**
```bash
caffeinate -dimsu python benchmark.py --benchmark-mode light --iterations 30 --start-iteration 1 --job-limit 1000
```

**Workload intensity variations:**
```bash
# Light workload (30 iterations per algorithm)
python benchmark.py --benchmark-mode light --iterations 30 --job-limit 1000

# Moderate workload (20 iterations per algorithm)  
python benchmark.py --benchmark-mode moderate --iterations 20 --job-limit 1000

# Heavy workload (20 iterations per algorithm)
python benchmark.py --benchmark-mode heavy --iterations 20 --job-limit 1000
```

**Why use `caffeinate -dimsu`?** The complete benchmark takes several hours to run all 15 algorithm combinations. The `caffeinate` command prevents macOS from sleeping during long runs.

In our experience,  each benchmark iteration of all 15 algorithms took ~52 minutes. For 30 benchmark trials, that would be equivalent to 26 hours of non-stop running.

### Individual Component Testing

**Single algorithm combination:**
```bash
python benchmark.py --single-run edf_job network_optimal_fair --iterations 5 --job-limit 200
```

**Development and validation:**
```bash
# Quick testing
python benchmark.py --benchmark-mode test --iterations 5 --job-limit 100

# Cold start analysis  
python benchmark.py --benchmark-mode cold_start_test --iterations 10 --job-limit 200
```

### Manual System Operation

**Start complete system manually:**
```bash
# Terminal 1: Start scheduler
python scheduler.py --job-algorithm edf_job --worker-algorithm network_optimal_fair

# Terminal 2-5: Start workers (4 workers recommended)  
python worker.py --worker_id WORKER_1
python worker.py --worker_id WORKER_2
python worker.py --worker_id WORKER_3
python worker.py --worker_id WORKER_4

# Terminal 6: Start job generator
python job-generator.py
```

## Benchmarking Methodology

### Three-Phase Protocol
1. **Warm-up Phase (30s):** System stabilisation period
2. **Measurement Phase (180s):** Primary data collection
3. **Cool-down Phase (37s):** System cleanup and metrics consolidation

### Workload Intensities

|Mode|Inter-arrival Delay|Jobs/Batch|Use Case|
|---|---|---|---|
|Light|8-15s|4|Low-frequency FaaS workloads|
|Moderate|2-5s|6|Standard production workloads|
|Heavy|0.5-1.5s|8|Peak traffic scenarios|

### Experimental Design
**Total Scope:**
- **15 algorithms** x **70 total trials** = **1,050 individual benchmark runs**
- **109,568 total jobs** evaluated across all experiments
- **Multiple validation approaches** ensuring literature compliance

## Statistical Evaluation Framework
The analysis employs a multi-layered statistical approach:

### Analysis Components
1. **Distribution Analysis:** Coefficient of Variation assessment
2. **Variance Component Analysis:** Two-way and Three-way ANOVA, Cohen's d
3. **Categorical Association Analysis:** Chi-square, Cramér's V, Standardised residuals
4. **Thermal Efficiency Analysis:** Container lifecycle management metrics
5. **Cross-Validation:** Correlation validation between independent analytical approaches

### Statistical Analysis Notebooks
The comprehensive statistical evaluation is documented in detailed Jupyter notebooks available on the submission page.

- **Part 1:** Distribution & Variance Analysis: 
- **Part 2:** Categorical Association Analysis
- **Part 3:** Thermal Efficiency & Cross-Validation

## Key Research Findings

### Primary Discoveries
1. **Worker Selection Dominance:** Worker algorithms demonstrate significantly stronger performance impact under light workloads (Cramér's V = 0.33) compared to job algorithms (p > 0.05, V < 0.01)
2. **Thermal State Hierarchy:** Container lifecycle management > worker selection > job selection, with penalties accounting for >98% of performance variation
3. **Workload Sensitivity Zones:** Moderate workloads demonstrate 87x greater algorithmic sensitivity, revealing optimal zones for meta-scheduling
4. **Cross-Validation Strength:** Thermal stability patterns predict penalty avoidance with strong correlation (r = -0.89, p < 0.001)

### Practical Implications
- **Meta-Scheduling Viability:** Framework successfully exposes interpretable trade-offs for adaptive algorithm selection
- **Performance Predictability:** Thermal efficiency metrics provide reliable scheduling optimisation indicators
- **Regime-Dependent Behaviour:** Different workload intensities require distinct scheduling approaches

## File Structure

```
meta_taskwave/
├── README.md                    # Project documentation
├── requirements.txt             # Python dependencies
├── config.py                    # System configuration
├── utils.py                     # Shared utilities and messaging
├── job.py                       # Job data structure
├── scheduler.py                 # Central scheduling coordinator  
├── worker.py                    # Worker node implementation
├── job-generator.py             # Workload generation
├── benchmark.py                 # Benchmarking orchestrator
├── benchmark_utils.py           # Metrics calculation and analysis
└── results/                     # Generated benchmark data
    └── benchmark_*/             # Timestamped experimental results
        ├── benchmark_results.csv
        ├── ml_consolidated_dataset.csv
        └── benchmark_progress.csv
```

## Research Contributions
1. **Transparent Framework:** First systematic comparison of hybrid scheduling strategies in FaaS environments
2. **Thermal Efficiency Innovation:** Novel metric connecting container lifecycle management to scheduling performance
3. **Reproducible Methodology:** Open-source implementation enabling comparative research
4. **Meta-Selection Foundation:** Structured insights supporting adaptive scheduling through interpretable algorithm selection
5. **Workload Regime Discovery:** Identification of optimal sensitivity zones for scheduling approaches

## Technical Specifications
### System Parameters

- **Job Workload Profile:** 4-8 jobs per batch, 200-600MB load variation
- **Worker Configuration:** 600-1200MB memory, 0.8-1.2x processing speed variation
- **Network Constraints:** 0.05-0.2s latency ranges with proximity-based routing
- **Cold Start Simulation:** 1:60 time compression (1 second in simulation represents 60 seconds in real FaaS environments), 60-second inactivity threshold
- **Experimental Scale:** 1,050 trials, 109,568 total jobs evaluated

### Performance Validation
All scheduling algorithms follow foundational computer science principles without complex optimisation heuristics, ensuring algorithmic effects can be isolated and compared systematically.

---

## References

AWS. (2025). *AWS Lambda resource model and configuration*. Amazon Web Services Documentation. https://docs.aws.amazon.com/lambda/

Chu, X., Li, M., & Qin, S. (2025). Dynamic load balancing using resource-aware task assignment in distributed systems. *IEEE Transactions on Cloud Computing*, 13(1), 45-62.

Copik, M., Kwasniewski, G., Besta, M., Podstawski, M., & Hoefler, T. (2021). SeBS: A serverless benchmark suite for function-as-a-service computing. *Proceedings of the 22nd International Middleware Conference*, 64-78.

Eismann, S., Scheuner, J., van Eyk, E., Schwinger, M., Grohmann, J., Herbst, N., ... & Kounev, S. (2022). The state of serverless applications: Collection, characterization, and community consensus. *IEEE Transactions on Software Engineering*, 48(10), 4152-4166.

Fuerst, A., & Sharma, P. (2021). FaasCache: Keeping serverless computing alive with greedy-dual caching. *Proceedings of the 26th ACM International Conference on Architectural Support for Programming Languages and Operating Systems*, 386-400.

Joosen, W., Bousselham, W., Vanbrabant, L., Coninck, E. D., Lagaisse, B., & Landuyt, D. V. (2023). Characterizing workload of production function-as-a-service platforms. *Journal of Systems and Software*, 201, 111614.

Kaur, P., Rani, R., & Aggarwal, H. (2020). KEIDS: Kubernetes-based energy and interference driven scheduler for industrial IoT in edge-cloud ecosystem. *IEEE Internet of Things Journal*, 7(5), 4228-4237.

Lin, C., Khazaei, H., & Litoiu, M. (2019). Performance modelling and optimization of serverless computing applications. *IEEE Transactions on Cloud Computing*, 10(3), 1523-1536.

Mahgoub, A., Tarraf, A., Shankar, M., Kourtellis, N., Hoque, I., & Chaterji, S. (2022). WISEFUSE: Workload characterization and DAG transformation for serverless workflows. *Proceedings of the ACM on Measurement and Analysis of Computing Systems*, 6(2), 1-28.

Mahmoudi, N., & Khazaei, H. (2022). Performance modeling of serverless computing platforms. *IEEE Transactions on Cloud Computing*, 10(4), 2834-2847.

Romero, F., Chaudhry, G. I., Goiri, I., Gopa, P., Batum, P., Yadwadkar, N. J., ... & Bianchini, R. (2021). FaaS profiler: Tracing serverless functions at scale. *Proceedings of the 2021 USENIX Annual Technical Conference*, 843-858.

Scheuner, J., & Leitner, P. (2020). Function-as-a-service performance evaluation: A multivocal literature review. *Journal of Systems and Software*, 170, 110708.

Shahrad, M., Fonseca, R., Goiri, I., Chaudhry, G., Batum, P., Cooke, J., ... & Bianchini, R. (2020). Serverless in the wild: Characterizing and optimizing the serverless workload at a large cloud provider. *Proceedings of the 2020 USENIX Annual Technical Conference*, 205-218.

Silva, P., Fireman, D., & Pereira, T. E. (2020). Prebaking functions to warm the serverless cold start. *Proceedings of the 21st International Middleware Conference*, 1-13.

Thinakaran, P., Gunasekaran, J. R., Sharma, B., Kandemir, M. T., & Das, C. R. (2019). Kube-Knots: Resource harvesting through dynamic container orchestration in GPU-based datacenters. *Proceedings of the 2019 IEEE International Conference on Cluster Computing*, 1-13.

Ustiugov, D., Petrov, P., Kogias, M., Bugnion, E., & Grot, B. (2021). Benchmarking, analysis, and optimization of serverless function snapshots. *Proceedings of the 26th ACM International Conference on Architectural Support for Programming Languages and Operating Systems*, 559-572.

Vandebon, R., Coutinho, R. D. S., & Luk, W. (2021). Multi-objective scheduling of serverless functions. *Proceedings of the 2021 IEEE International Conference on Cloud Engineering*, 134-144.

Wang, L. (2024). Performance evaluation of cloud scheduling algorithms using statistical methods. *ACM Computing Surveys*, 56(3), 1-35.

Yin, J., Zhao, L., & Wang, H. (2024). Static task allocation for Kubernetes in real-time systems. *IEEE Transactions on Cloud Computing*, 12(2), 512-528.

---

## Attribution & Contact

**Author:** Theresa Rivera To  
**Institution:** Queen Mary University of London
**Academic Year:** 2024-2025  
**Supervisor:** Dr. Chris Phillips  

### Repository Information
- **License:** MIT License (see [LICENSE](LICENSE) file)
- **Citation:** If you use this work in your research, please cite:
  ```bibtex
  @software{metataskwave2025,
    author = {To, Theresa Rivera},
    title = {Meta TaskWave: Meta-Selection for Hybrid Dynamic Scheduling in FaaS Environments},
    year = {2025},
    publisher = {GitHub},
    url = {https://github.com/theresarto/project-taskwave}
  }

### Contact
**GitHub:** @theresarto<br>
**LinkedIn:** https://www.linkedin.com/in/theresa-to-358728105/ <br>
**Email:** theresarto@gmail.com <br>

### Acknowledgements
This project was completed as part of the MSc Computing and Information Systems programme. Special thanks to Dr. Chris Phillips for supervision and guidance.

### Disclaimer
This is an academic research project. The code is provided as-is for educational and research purposes.
