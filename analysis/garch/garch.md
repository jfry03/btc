---
title: "GARCH, explained on BTC"
subtitle: "Why returns aren't iid, what GARCH does about it, and what it buys you for a 5-minute market"
date: "September 2026"
geometry: margin=2.2cm
fontsize: 11pt
colorlinks: true
header-includes:
  - \usepackage{booktabs}
  - \usepackage{float}
  - \floatplacement{figure}{H}
---

# 1. The problem GARCH solves

Take BTCUSDT (Binance perp) mid-price log returns on a 1-minute grid. Two facts, both visible in
Figure 1:

1. **The sign of the next return is unpredictable.** The autocorrelation of $r_t$ is zero at every lag
   (Figure 2, left). Returns are a martingale difference: uncorrelated, mean zero.
2. **The size of the next return is predictable.** Big moves cluster. The autocorrelation of $r_t^2$
   starts at 0.2 and is still positive two hours later (Figure 2, right).

![A day of 1-minute returns. The top panel looks like noise; the bottom panel shows the noise has a
volume knob that somebody keeps turning.](fig1_clustering.png){width=95%}

![Left: returns are uncorrelated. Right: squared returns are strongly and persistently autocorrelated.
This pair of pictures is the entire motivation for GARCH.](fig2_acf.png){width=95%}

Uncorrelated but not independent. Any model with iid returns (a random walk with constant
$\sigma$, for instance) gets the first fact right and the second one completely wrong, and the
consequence shows up in the tails: summed over $k$ steps, iid returns have excess kurtosis
$\kappa_1/k$, which for BTC would predict $\approx 0.7$ at 5 minutes. The measured value over 495
days is $\approx 120$.

## The "volume knob" picture

The way to hold this in your head is

$$ r_t = \sigma_t \, \varepsilon_t $$

where $\varepsilon_t$ is a standardised shock (mean 0, variance 1, roughly iid) and $\sigma_t$ is the
*current* volatility - a slowly moving, persistent quantity. The shock is the coin flip; $\sigma_t$
is how much money is riding on it. A 5-minute return is $\sum \sigma_s \varepsilon_s \approx \sigma_t
\sum \varepsilon_s$, i.e. one Gaussian-ish draw scaled by whatever $\sigma$ happened to be in force.
Pool a year of those and you get a **mixture of Gaussians with different widths** - a sharp peak
(calm windows) with fat tails (wild windows). The CLT never gets to work because within any short
window you are stuck inside one $\sigma$ regime.

GARCH is nothing more than a specific, fittable recipe for $\sigma_t$.

# 2. The model

## ARCH (Engle, 1982)

The simplest recipe: today's variance is a weighted average of recent squared returns,

$$ \sigma_t^2 = \omega + \alpha_1 r_{t-1}^2 + \alpha_2 r_{t-2}^2 + \dots + \alpha_q r_{t-q}^2 . $$

If yesterday's move was big, today's expected move is big. It works, but you need many lags to
capture the slow decay in Figure 2, and every lag is a parameter.

## GARCH(1,1) (Bollerslev, 1986)

Add one lag of the variance itself:

$$ \boxed{\;\sigma_t^2 = \omega + \alpha\, r_{t-1}^2 + \beta\, \sigma_{t-1}^2\;} $$

Three parameters. Read it as: *new variance = a constant + a bit of the latest surprise + most of
the old variance*. Substituting recursively,

$$ \sigma_t^2 = \frac{\omega}{1-\beta} + \alpha \sum_{j=1}^{\infty} \beta^{\,j-1} r_{t-j}^2 , $$

so GARCH(1,1) is an **exponentially-weighted moving average of squared returns**, with decay
$\beta$ per step, plus a constant that pulls it back toward a long-run level. It is ARCH with
infinitely many lags and geometrically decaying weights, for the price of one extra parameter.

Useful quantities that follow directly:

| quantity | formula | meaning |
|---|---|---|
| long-run variance | $\bar\sigma^2 = \omega / (1-\alpha-\beta)$ | where $\sigma_t^2$ mean-reverts to |
| persistence | $\alpha+\beta$ | fraction of a variance shock that survives one step |
| half-life | $\ln 0.5 / \ln(\alpha+\beta)$ | steps for a shock to decay by half |
| reactivity | $\alpha$ | how much one surprise moves the estimate |
| $h$-step forecast | $\bar\sigma^2 + (\alpha+\beta)^{h-1}(\sigma_{t+1}^2 - \bar\sigma^2)$ | geometric decay to the long-run level |
| kurtosis (Gaussian $\varepsilon$) | $3\,\dfrac{1-(\alpha+\beta)^2}{1-(\alpha+\beta)^2-2\alpha^2}$ | fat tails from vol variation alone |

Constraints: $\omega>0$, $\alpha,\beta\ge0$ for positivity; $\alpha+\beta<1$ for a finite long-run
variance. When $\alpha+\beta=1$ the model is IGARCH: variance shocks never decay and the long-run
level is undefined (the RiskMetrics EWMA is IGARCH with $\omega=0$, $\beta=0.94$).

## What it looks like

Figure 3 simulates 5 000 steps of iid Gaussian noise and of GARCH(1,1) with the *same* unconditional
variance. The GARCH path has visible bursts and lulls; its histogram is peaked with heavy tails
(excess kurtosis 3.3 from the formula above) even though every individual $\varepsilon_t$ is
Gaussian. That is fat tails manufactured purely by vol clustering.

![Same long-run variance, very different behaviour. The grey line in the lower left panel is
$\pm\sigma_t$: the model's own volume knob.](fig3_sim.png){width=95%}

# 3. Fitting it

The parameters come from maximum likelihood. Given $(\omega,\alpha,\beta)$, run the recursion
forward to get every $\sigma_t^2$, then the (Gaussian) log-likelihood is

$$ \ell = -\tfrac12 \sum_t \left[\ln(2\pi\sigma_t^2) + \frac{r_t^2}{\sigma_t^2}\right] , $$

and you maximise it numerically. The Gaussian likelihood gives consistent estimates of
$(\omega,\alpha,\beta)$ even when the true shocks are fat-tailed (this is "QMLE"); the shape of
$\varepsilon$ can be fitted afterwards to the standardised residuals $z_t = r_t/\sigma_t$.

\newpage

## On BTC 1-minute returns

Fitted on 291 consecutive days (June 2023 - March 2024, 419k minutes) of 1-minute mid returns:

| | value |
|---|---|
| $\alpha$ | 0.082 |
| $\beta$ | 0.916 |
| $\alpha+\beta$ | 0.9976 |
| half-life | 283 min ($\approx$ 4.7 h) |
| long-run $\sigma$ | 6.6 bp / min ($\approx$ 40% annualised) |
| Student-t $\nu$ for $z$ | 4.9 |

A note on how these were obtained, because it matters in practice. Unconstrained MLE at 1-minute
frequency pushes $\alpha+\beta$ to 1 (IGARCH): the likelihood keeps improving as persistence goes to
0.9999, at which point the "long-run variance" comes out at 5x the sample variance and is
meaningless. This is a well-known artefact of high-frequency data: the vol regime drifts over weeks
and months, and the only way a GARCH(1,1) can track a drifting level is to give up on mean
reversion. The fix used here is **variance targeting** - pin $\omega$ so the long-run variance
equals the sample variance and estimate only $\alpha,\beta$ - which costs almost nothing in
likelihood and gives an interpretable model. (Fitting on one contiguous era rather than days
sampled across three years matters for the same reason.)

![Fitted one-step-ahead $\sigma_t$ against realised $|r_t|$ over three days. The estimate rises
within a minute or two of a burst and takes hours to come back down.](fig4_fitted_sigma.png){width=95%}

## Does it work?

The job of the model is to make $z_t = r_t/\sigma_t$ look iid. Three checks, before and after:

| | raw $r_t$ | $z_t = r_t/\sigma_t$ | iid Gaussian |
|---|---|---|---|
| lag-1 autocorrelation of squares | 0.37 | 0.003 | 0 |
| excess kurtosis | 240 | 101 | 0 |
| $P(|z|>3)$ | 1.5% | 1.2% | 0.27% |
| $P(|z|>5)$ | 0.34% | 0.17% | 0.00006% |

The clustering is gone: the squared standardised residuals are white. The tails are only half
gone. A 5$\sigma$ minute still happens once every ~600 minutes instead of once every 1.7 million.
Figure 5 shows why: the standardised residuals are not Gaussian at all - they sit on a Student-t
with about 5 degrees of freedom. This is the normal state of affairs. GARCH with a Gaussian
$\varepsilon$ explains kurtosis of order 3-10; the rest has to come from a fat-tailed
$\varepsilon$ (Student-t is the default) or an explicit jump component.

![Standardising by the GARCH $\sigma_t$ pulls the distribution in (orange vs blue) but leaves it far
from Gaussian (dashed). A Student-t with $\nu\approx5$ (green) fits the residual shape
well.](fig5_residuals.png){width=95%}

# 4. Forecasting

The one-step forecast is just the recursion: $\hat\sigma_{t+1}^2 = \omega + \alpha r_t^2 + \beta
\sigma_t^2$. Multi-step forecasts decay geometrically toward the long-run level,

$$ E_t[\sigma_{t+h}^2] = \bar\sigma^2 + (\alpha+\beta)^{h-1}\left(\hat\sigma_{t+1}^2-\bar\sigma^2\right), $$

and the forecast variance of a $k$-step return is the sum of those. Figure 6 shows the term
structure from two starting states. Two things to notice:

- With a half-life of ~5 hours, **the 5-minute-ahead vol is essentially the current vol.** For a
  5-minute market the mean-reversion term is irrelevant; what matters is getting $\sigma_t$ right
  *now*.
- After a burst, the forecast for the next few hours stays elevated. The model's memory is what
  makes it useful; it is also what makes it slow to notice the burst is over.

![Variance forecasts from a high-vol and a low-vol starting point. The green sliver at the left is
the 5-minute horizon: for that purpose the forecast is flat.](fig6_forecast.png){width=95%}

# 5. What plain GARCH gets wrong, and the usual fixes

**Intraday seasonality.** BTC trades 24/7 but not evenly: 1-minute vol at 13:00-16:00 UTC is
double the 04:00-10:00 level (Figure 7). GARCH treats that predictable rise as a "shock" every
afternoon and then slowly forgets it every night. Standard fix: estimate the time-of-day profile
$s(\tau)$, fit GARCH to $r_t/s(\tau_t)$, multiply back for forecasts.

![Root-mean-square 1-minute return by hour of day, over 125 days. This is deterministic and known
in advance, and a plain GARCH ignores it.](fig7_seasonality.png){width=95%}

**Leverage / asymmetry.** In equities negative returns raise vol more than positive ones; GJR-GARCH
adds a term $\gamma\, r_{t-1}^2\,\mathbf{1}[r_{t-1}<0]$, EGARCH models $\ln\sigma^2$ with a sign
term. For BTC at intraday horizons the asymmetry is weaker and both directions of liquidation
cascade exist, but it is worth testing.

**Reaction speed.** GARCH updates once per bar using one squared return - a noisy signal. If you
have finer data, a *realised variance* computed from many sub-bar returns (e.g. the sum of squared
1-second returns over the last 15 minutes) is a far better vol estimate than anything a 1-minute
GARCH can extract from 1-minute bars. On this dataset, scaling 5-minute returns by trailing
15-minute realised vol from 1-second data cut excess kurtosis from 170 to ~50, better than the
1-minute GARCH manages. Models built around this idea are HAR-RV (Corsi, 2009) and
realised-GARCH (Hansen et al., 2012). GARCH is what you use when you *only* have the bars.

**Jumps.** No vol model removes the 0.2% of windows that are news or cascades. Those need a
fat-tailed innovation ($\nu\approx3$-5) or an explicit jump process, and the tails of the
prediction should be read as "at least this fat".

# 6. What this means for a 5-minute BTC binary

The probability that BTC ends the window above the strike is, under any of these models,
essentially a function of

$$ d = \frac{\ln(K / P_t)}{\hat\sigma\,\sqrt{T-t}} , $$

the distance to the strike in forecast-vol units. Everything above is about producing
$\hat\sigma$ and the shape of the distribution around it:

1. **Level.** Use a fast, floored estimate of *current* vol - trailing 15-30 min realised vol from
   the 1-second grid, or a GARCH(1,1) on 1-minute bars with variance targeting if that is all
   you have. Ignore mean reversion at this horizon. Correct for time-of-day.
2. **Shape.** Do not use a Gaussian for the conditional distribution. Student-t with
   $\nu\approx4$-5 on the standardised residual reproduces the observed tails; a Gaussian
   underprices a 4$\sigma$ state by two orders of magnitude.
3. **Where the edge lives.** Getting the level right is most of the value. The shape matters
   when the price is already several $\hat\sigma$ from the strike with time left: a Gaussian says
   "99.9%", the data says "99%". Whether that residual 1% survives fees is a separate question.

# References

- Engle, R. (1982). Autoregressive conditional heteroscedasticity with estimates of the variance of UK inflation. *Econometrica*.
- Bollerslev, T. (1986). Generalized autoregressive conditional heteroskedasticity. *J. Econometrics*.
- Andersen, T. & Bollerslev, T. (1997). Intraday periodicity and volatility persistence in financial markets. *J. Empirical Finance*.
- Corsi, F. (2009). A simple approximate long-memory model of realized volatility. *J. Financial Econometrics*.
- Hansen, P., Huang, Z. & Shek, H. (2012). Realized GARCH. *J. Applied Econometrics*.

*Code: `analysis/garch/make_figures.py` (figures, fit, `fitted.json`), `analysis/cond_kurt.py`
(realised-vol standardisation), `analysis/return_dists.py` (return distributions).*
