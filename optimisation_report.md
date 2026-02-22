The Problem

No credible open-source implementation of the insurance price optimisation stack exists anywhere. Commercial tools (WTW Radar Optimiser, Akur8 Optim, Earnix) cost over $100k per year. The academic literature is mature and public, meaning the algorithms are well-documented. This is the single largest gap in actuarial open-source tooling.
Price optimisation is how insurers decide what price to charge each customer, given their risk cost, the competitive market, customer price sensitivity, and business constraints (retention targets, combined ratio caps, regulatory limits). Every insurer with more than a few thousand policies needs this capability.

Who Needs This

The UK MGA market alone contains 350+ MGAs (240 MGAA members, GWP of £13.2B), 2,949 Lloyd's coverholders, and sub-£100M GWP MGAs that have zero price optimisation capability. Industry survey data (hx/Coleman Parkes 2024, n=350) shows 76% of actuaries take over 30 days to build a new pricing model, and 81% say they lack the right pricing platform.
The TAM estimate for this market is £10-30M ARR, including Lloyd's coverholders.

Research Findings

The Regulatory Landscape

Approximately 18 US states have banned individual demand-based price optimisation (California, New York, Florida, Maryland, Ohio, among others). In the UK, FCA PS21/5 (January 2022) bans renewal price walking: the renewal price must be less than or equal to the Equivalent New Business Price (ENBP) by channel by product. However, new business price optimisation remains fully permitted in the UK. EIOPA (2023) issued a supervisory statement against price walking but not a hard ban on optimisation. The EU AI Act (August 2024) classifies AI pricing in life and health as "high risk" requiring transparency.
FCA EP25/2 (December 2025) confirms that price walking has been "largely eliminated in motor," saving consumers approximately £1.6B. However, an emerging concern is that firms are adjusting product quality instead of price. New Consumer Duty implications may follow.

The Mathematical Framework

The price optimisation problem follows a well-documented sequence. Step 1: fit a risk model (Poisson/Gamma GLMs) to compute pure premium per risk. Step 2: fit a demand model (Binomial/logistic GLM on quote-conversion data) to estimate conversion probability as a function of price, customer features, and competitor prices. Step 3: optimise the portfolio by maximising expected profit subject to constraints (retention floor, combined ratio cap, rate change bounds, price must exceed a fraction of technical price). Step 4: trace the efficient frontier by sweeping constraints to produce a profit vs volume Pareto curve. Step 5: the actuary selects a point on the frontier consistent with business strategy.
Key academic references: Emms and Haberman (ASTIN Bulletin, 2005) for the canonical continuous-time treatment, Guven and McPhail (CAS E-Forum, Spring 2013) for the best practitioner treatment, Murphy, Brockman, and Lee (CAS Forum, Winter 2000) for foundational dynamic pricing with GLMs.
Recent academic work includes arXiv 2512.03242 (2025) proving a closed-form relationship between risk model accuracy and portfolio loss ratio under price elasticity. The key result is that improvements to poor risk models yield larger loss ratio gains than improvements to already-good models. Also: OptiGrad (arXiv 2404.10275, 2024) for gradient-based optimisation with differentiable demand, and a fairness-aware framework (arXiv 2512.24747, December 2025) using NSGA-II multi-objective optimisation.

The ENBP Compliance Checker (Recommended Entry Point)

The ENBP compliance checker does not exist in open source. Every UK personal lines insurer (all motor and home writers) must prove that their renewal price is less than or equal to the Equivalent New Business Price by channel by product. This is an immediate, uncontroversial, zero-regulatory-risk starting point for the optimisation stack. It requires only the ability to compare renewal quotes against a new business rating model — no demand modelling, no optimisation, no controversy.

Proposed Build (Phased)

Phase 1: ENBP Compliance Checker (2 weeks)

Build a module that takes a renewal portfolio and a new business rating model, computes the ENBP for each policy, flags violations, and produces a compliance report. This has immediate demand from every UK motor and home insurer, involves zero regulatory controversy, and establishes PF in the price optimisation space.
The technical implementation uses polars DataFrames, the existing rustystats GLM predictions, and simple comparison logic. The output is a compliance report DataFrame with columns: policy ID, renewal price, ENBP, compliant (boolean), margin.

Phase 2: Demand Model Fitting (3 weeks)

Build a module for fitting logistic GLMs on quote-conversion data. Key features: log(price_ratio) as the price variable, separation of new business vs renewal models, elasticity diagnostics and lift charts. This uses rustystats' existing binomial GLM family.
The demand model is where the data availability risk lives. Many insurers and MGAs do not have quote-level conversion data. The module should work with whatever data is available and degrade gracefully.

Phase 3: Price Experiment Design (2 weeks)

Statistical power analysis for randomised price tests. Treatment assignment with randomisation. This is a novel contribution: no OSS tool exists for designing insurance pricing experiments. Implementation uses scipy.stats for power calculations.

Phase 4: Portfolio Optimiser (3 weeks)

The core solver: scipy.optimize SLSQP for continuous problems, optionally cvxpy for convex relaxations or pymoo for multi-objective optimisation. Jurisdiction-appropriate default constraints baked in. The efficient frontier visualisation module traces the profit-volume Pareto curve.

Architecture Question

Should this be built as a 4th standalone PF tool, or as a haute pipeline stage? The argument for standalone: it has its own identity and discovery surface area on PyPI. The argument for pipeline stage: price optimisation is always the last step before quote issuance, which is where haute already operates. The recommendation is to start as a standalone module and integrate with haute later.

Dependencies

* Phase 1 (ENBP): Requires rustystats GLM predictions. No other dependencies.
* Phase 2 (Demand): Requires rustystats binomial GLM. No other dependencies.
* Phase 3 (Experiments): Standalone. scipy only.
* Phase 4 (Optimiser): Requires Phase 2 demand model output.

Key Risks

* Data availability: the demand model needs quote-level conversion data that many insurers do not have.
* US regulatory: the library itself is not illegal, but certain applications are banned in approximately 18 US states. Correct labelling and documentation are required.
* Adoption: needs tutorial notebooks with realistic examples (synthetic data) to convert curiosity into use.

Open Questions

* Should the ENBP checker be the first thing built, or should Ralph start with the demand model?
* Standalone PyPI package or rustystats extension?
* What synthetic dataset should be used for demos (real quote data is proprietary)?

