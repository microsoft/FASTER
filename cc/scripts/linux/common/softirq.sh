#!/bin/bash
#
# Pins network softirqs to cores 0-7. Requires root permission.

# Given a network interface, finds the set of softirqs and sets their
# affinity to cores 0-7.
pin_softirqs() {
    irqs=$(cat /proc/interrupts | grep "$1" | awk -F : '{ print $1 }')

    for irq in ${irqs[@]}; do
        echo 0-7 > /proc/irq/"$irq"/smp_affinity_list
    done
}

# Pin softirqs of all standard network interfaces in linux.
pin_softirqs eno
pin_softirqs eth
pin_softirqs mlx
