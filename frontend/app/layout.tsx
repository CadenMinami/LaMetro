import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'LA Metro Live',
  description: 'Real-time LA Metro vehicle positions and reliability.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
