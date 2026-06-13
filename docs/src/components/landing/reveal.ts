import type { Variants } from "motion/react";

// shared staggered reveal used by every landing section
export const reveal: Variants = {
  visible: (i: number) => ({
    filter: "blur(0px)",
    y: 0,
    opacity: 1,
    transition: {
      delay: i * 0.12,
      duration: 0.55,
      ease: [0.22, 1, 0.36, 1],
    },
  }),
  hidden: {
    filter: "blur(10px)",
    y: 18,
    opacity: 0,
  },
};
