import EquityMap from '@/components/EquityMap';
import { AccountNav } from '@/components/AccountNav';

export default function EquityPage() {
  return (
    <div className="relative h-screen w-screen overflow-hidden">
      <EquityMap />
      <AccountNav />
    </div>
  );
}
