
use std::sync::Arc;

use rustfft::{num_complex::Complex, Fft, FftPlanner};

pub const FFT: usize = 2048;
pub const HOP: usize = 1024;
pub const BANDS: usize = 32;

/// Band edges as FFT bin indices, BANDS + 1 values.
pub fn band_edges(rate: u32) -> [usize; BANDS + 1] {
    let a = (50.0_f64).log10();
    let b = (16000.0_f64).log10();
    let mut out = [0usize; BANDS + 1];
    let mut prev = 1usize;
    for (i, slot) in out.iter_mut().enumerate() {
        let freq = 10f64.powf(a + i as f64 * (b - a) / 32.0);
        let bin = (freq / rate as f64 * FFT as f64) as usize;
        *slot = bin.clamp(1, FFT / 2).max(prev);
        prev = *slot;
    }
    out
}

pub struct Spectrum {
    buf: Vec<f32>,
    window: Vec<f32>,
    scale: f32,
    edges: [usize; BANDS + 1],
    fft: Arc<dyn Fft<f32>>,
    last_sample: Option<f64>,
    last_advance: Option<f64>,
    /// Latest analysed level per band, 0.0..=1.0.
    pub targets: [f32; BANDS],
    /// Displayed bar height per band, 0.0..=1.0.
    pub levels: [f32; BANDS],
    /// Falling peak cap per band, 0.0..=1.0.
    pub peaks: [f32; BANDS],
}

impl Spectrum {
    pub fn new(rate: u32) -> Self {
        let window: Vec<f32> = (0..FFT)
            .map(|n| (0.5 - 0.5 * ((2.0 * std::f64::consts::PI * n as f64 / (FFT - 1) as f64).cos())) as f32)
            .collect();
        let scale = window.iter().sum::<f32>() / 2.0;
        let fft = FftPlanner::<f32>::new().plan_fft_forward(FFT);
        Self {
            buf: Vec::new(),
            window,
            scale,
            edges: band_edges(rate),
            fft,
            last_sample: None,
            last_advance: None,
            targets: [0.0; BANDS],
            levels: [0.0; BANDS],
            peaks: [0.0; BANDS],
        }
    }

    /// Mono samples in -1.0..=1.0. Returns true if at least one FFT ran.
    pub fn add_samples(&mut self, samples: &[f32], now: f64) -> bool {
        self.last_sample = Some(now);
        self.buf.extend_from_slice(samples);
        let mut ran = false;
        while self.buf.len() >= FFT {
            let chunk = &mut self.buf[..FFT];
            let mut c: Vec<Complex<f32>> = chunk
                .iter()
                .zip(&self.window)
                .map(|(s, w)| Complex::new(*s * *w, 0.0))
                .collect();
            self.fft.process(&mut c);
            let mut mags = [0.0f32; FFT / 2 + 1]; // bins 0..=FFT/2: an edge may sit on the last one
            for (k, v) in mags.iter_mut().zip(&c) {
                *k = v.norm() / self.scale;
            }
            for (j, target) in self.targets.iter_mut().enumerate() {
                let a = self.edges[j];
                let b = self.edges[j + 1];
                let value = if b > a {
                    mags[a..b].iter().cloned().fold(0.0_f32, f32::max)
                } else {
                    mags[a]
                };
                let db = 20.0 * (value + 1e-9).log10();
                *target = ((db + 60.0) / 60.0).clamp(0.0, 1.0);
            }
            self.buf.drain(..HOP);
            ran = true;
        }
        ran
    }

    /// Animate levels and peaks towards the targets. Returns true if any level or peak changed.
    pub fn advance(&mut self, now: f64) -> bool {
        if self.last_advance.is_none() {
            self.last_advance = Some(now);
            return false;
        }
        let elapsed = (now - self.last_advance.unwrap()).clamp(0.0, 0.25);
        self.last_advance = Some(now);
        if self.last_sample.is_none() || now - self.last_sample.unwrap() > 0.25 {
            self.targets.fill(0.0);
        }
        if elapsed == 0.0 {
            return false;
        }
        let mut changed = false;
        for j in 0..BANDS {
            let before_l = self.levels[j];
            let before_p = self.peaks[j];
            let rate = if self.targets[j] > self.levels[j] { 16.0 } else { 4.0 };
            let alpha = (1.0 - (-rate * elapsed).exp()) as f32;
            self.levels[j] += (self.targets[j] - self.levels[j]) * alpha;
            if (self.targets[j] - self.levels[j]).abs() < 0.001 {
                self.levels[j] = self.targets[j];
            }
            self.peaks[j] = self.levels[j].max(self.peaks[j] - 0.75f32 * elapsed as f32);
            if self.levels[j] != before_l || self.peaks[j] != before_p {
                changed = true;
            }
        }
        changed
    }

    pub fn is_idle(&self, now: f64) -> bool {
        self.targets.iter().all(|&t| t <= 0.001)
            && self.levels.iter().all(|&l| l <= 0.001)
            && self.peaks.iter().all(|&p| p <= 0.001)
            && (self.last_sample.is_none() || now - self.last_sample.unwrap() > 0.25)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sine(freq: f64, amp: f64) -> Vec<f32> {
        (0..FFT)
            .map(|n| (amp * (2.0 * std::f64::consts::PI * freq * n as f64 / 44100.0).sin()) as f32)
            .collect()
    }

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 0.003
    }

    #[test]
    fn s1_band_edges() {
        assert_eq!(
            band_edges(44100),
            [
                2, 2, 3, 3, 4, 5, 6, 8, 9, 11, 14, 16, 20, 24, 28, 34, 41, 49, 59, 71, 85, 102,
                122, 146, 175, 210, 251, 301, 361, 432, 518, 620, 743
            ]
        );
        assert_eq!(band_edges(8000)[32], 1024);
    }

    #[test]
    fn s2_window_scale() {
        let s = Spectrum::new(44100);
        assert!((s.scale - 511.75).abs() < 0.01);
        assert_eq!(s.window[0], 0.0);
        assert!(close(s.window[1023], 1.0));
        assert_eq!(s.window.len(), 2048);
    }

    #[test]
    fn s3_silence() {
        let mut s = Spectrum::new(44100);
        assert!(s.add_samples(&vec![0.0; 2048], 1.0));
        assert!(s.targets.iter().all(|&t| t == 0.0));
        // Low rates clamp edges to the last bin; it must be readable.
        assert!(Spectrum::new(8000).add_samples(&vec![0.0; 2048], 1.0));
    }

    #[test]
    fn s3_sine_1k() {
        let mut s = Spectrum::new(44100);
        assert!(s.add_samples(&sine(1000.0, 1.0), 1.0));
        let expected = [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0263,
            0.9817, 0.4477, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ];
        for (j, (got, want)) in s.targets.iter().zip(&expected).enumerate() {
            assert!(close(*got, *want), "band {}: got {}, want {}", j, got, want);
        }
    }

    #[test]
    fn s3_sine_100() {
        let mut s = Spectrum::new(44100);
        s.add_samples(&sine(100.0, 1.0), 1.0);
        let expected = [
            0.4246, 0.4246, 0.671, 0.671, 0.9602, 0.9881, 0.8005, 0.3049, 0.1846, 0.0129, 0.0,
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            0.0, 0.0, 0.0, 0.0,
        ];
        for (j, (got, want)) in s.targets.iter().zip(&expected).enumerate() {
            assert!(close(*got, *want), "band {}: got {}, want {}", j, got, want);
        }
    }

    #[test]
    fn s3_half_amplitude() {
        let mut s = Spectrum::new(44100);
        s.add_samples(&sine(1000.0, 0.5), 1.0);
        assert!(close(s.targets[16], 0.8814));
    }

    #[test]
    fn s3_hop() {
        let mut s = Spectrum::new(44100);
        let sig = sine(1000.0, 1.0);
        assert!(!s.add_samples(&sig[..1024], 1.0));
        assert!(s.add_samples(&sig[1024..], 1.0));
        assert!(close(s.targets[16], 0.9817));
        assert!(!s.add_samples(&vec![0.0; 1023], 1.0));
        assert!(s.add_samples(&[0.0], 1.0));
    }

    #[test]
    fn s4_attack_then_stall() {
        let mut s = Spectrum::new(44100);
        s.add_samples(&sine(1000.0, 1.0), 10.0);
        assert!(!s.advance(10.0));
        assert!(s.advance(10.1));
        assert!(close(s.levels[16], 0.7835));
        assert!(close(s.peaks[16], 0.7835));
        assert_eq!(s.levels[0], 0.0);
        assert!(!s.advance(10.1));
        assert!(s.advance(10.6));
        assert!(s.targets.iter().all(|&t| t == 0.0));
        assert!(close(s.levels[16], 0.2882));
        assert!(close(s.peaks[16], 0.5960));
    }

    #[test]
    fn s4_settles() {
        let mut s = Spectrum::new(44100);
        s.add_samples(&sine(1000.0, 1.0), 10.0);
        s.advance(10.0);
        s.add_samples(&sine(1000.0, 1.0), 10.25);
        assert!(s.advance(10.25));
        assert!(close(s.levels[16], 0.9637));
        s.add_samples(&sine(1000.0, 1.0), 10.5);
        assert!(s.advance(10.5));
        assert_eq!(s.levels[16], s.targets[16]);
        assert_eq!(s.peaks[16], s.levels[16]);
    }

    #[test]
    fn s5_idle() {
        let mut s = Spectrum::new(44100);
        assert!(s.is_idle(0.0));
        s.add_samples(&sine(1000.0, 1.0), 10.0);
        assert!(!s.is_idle(10.0));
        assert!(!s.is_idle(11.0));
        assert!(!s.advance(11.0));
        assert!(!s.advance(11.2));
        assert!(s.is_idle(11.2));
    }
}
